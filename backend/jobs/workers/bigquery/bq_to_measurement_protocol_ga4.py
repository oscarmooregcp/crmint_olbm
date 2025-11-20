# Copyright 2021 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""CRMint's workers to stream data from BigQuery to Google Analytics.

We stream data from BigQuery to the Measurement Protocol API, which doesn't
need credentials to authenticate calls, instead it uses a secret key to
control access.
"""

import json
import math
import string
import urllib

from google.api_core import page_iterator
import requests

from jobs.workers import worker
from jobs.workers.bigquery import bq_worker
from jobs.workers.ga import ga_utils

from urllib.parse import urlparse, urlunparse

class BQToMeasurementProtocolGA4(bq_worker.BQWorker):
  """Reads a BigQuery table of arbitraty size and schedule processing tasks.

  This worker reads the table by chunks of `BQ_BATCH_SIZE` rows and feed these
  rows into a processing worker of type `BQToMeasurementProtocolProcessorGA4`.
  This ensures that we never timeout on large tables, especially since Pub/Sub
  might be very sensitive to long running tasks not returning a status quickly
  enough.

  If we enqueued more than `MAX_ENQUEUED_JOBS` processing tasks, we stop
  enqueuing new processing tasks and schedule a new `BQToMeasurementProtocolGA4`
  worker with the `bq_page_token` parameter pointing to the next page to read
  from. This also ensures that we never timeout on large tables.
  """

  PARAMS = [
      ('bq_project_id', 'string', False, '', 'BQ Project ID'),
      ('bq_dataset_id', 'string', True, '', 'BQ Dataset ID'),
      ('bq_table_id', 'string', True, '', 'BQ Table ID'),
      ('bq_dataset_location', 'string', True, '', 'BQ Dataset Location'),
      ('measurement_id', 'string', True, '', ('Measurement ID / '
                                              'Firebase App ID')),
      ('api_secret', 'string', True, '', 'API Secret'),
      ('template', 'text', True, '', ('GA4 Measurement Protocol '
                                      'JSON template')),
      ('mp_batch_size', 'number', True, 20, ('Measurement Protocol '
                                             'batch size')),
      ('debug', 'boolean', True, False, 'Debug mode'),

  ]



  # BigQuery batch size for querying results.
  BQ_BATCH_SIZE = 2000

  # Maximum number of jobs to enqueued before spawning a new scheduler.
  MAX_ENQUEUED_JOBS = 100

  def _execute(self) -> None:
    client = self._get_client()
    bq_project_id = self._params['bq_project_id']
    bq_dataset_id = self._params['bq_dataset_id']
    dataset = client.get_dataset(f'{bq_project_id}.{bq_dataset_id}')
    page_token = self._params.get('bq_page_token', None)
    row_iterator = client.list_rows(
        dataset.table(self._params['bq_table_id']),
        page_token=page_token,
        page_size=self.BQ_BATCH_SIZE)

    enqueued_jobs_count = 0
    for _ in row_iterator.pages:
      # Enqueue job for this page
      worker_params = self._params.copy()
      worker_params['bq_page_token'] = page_token
      worker_params['bq_batch_size'] = self.BQ_BATCH_SIZE
      self._enqueue('BQToMeasurementProtocolProcessorGA4', worker_params, 0)
      enqueued_jobs_count += 1

      # Updates the page token reference for the next iteration.
      page_token = row_iterator.next_page_token

      # Spawns a new job to schedule the remaining pages.
      if (enqueued_jobs_count >= self.MAX_ENQUEUED_JOBS
          and page_token is not None):
        worker_params = self._params.copy()
        worker_params['bq_page_token'] = page_token
        self._enqueue(self.__class__.__name__, worker_params, 0)
        return


class BQToMeasurementProtocolProcessorGA4(bq_worker.BQWorker):
  """Reads the provided table chunk and stream it to Measurement Protocol API.

  A chunk is fully determined by two parameters: `bq_page_token` and
  `bq_batch_size`. This worker will read this given chunk and stream its
  content to the Measurement Protocol API for GA4 Properties.
  """
# Sends payload to Batch endpoint
  def _send_payload(self, payload, url: str) -> None:
    querystring = urllib.parse.urlencode({
        'measurement_id': self._params['measurement_id'],
        'api_secret': self._params['api_secret'],
    })
    response = requests.post(f'{url}?{querystring}',
                             data=json.dumps(payload),
                             headers={'content-type': 'application/json'})
    # A successful non-debug call returns 204, a debug call returns 200.
    # Any other status code is an error.
    if response.status_code == requests.codes.ok and self._params['debug']:
      for msg in response.json()['validationMessages']:
        self.log_warn(f'Validation Message: {msg["description"]}, '
                      f'Payload: {payload}')
    elif response.status_code != requests.codes.no_content:
      raise worker.WorkerException(f'Failed to send event. Status: '
                                   f'{response.status_code}, '
                                   f'Response: {response.text}')





  def _stream_rows(self, page: page_iterator.Page, batch_url: str) -> None:
    # Warns users if they are using an unsupported formatting syntax.
    if '%(' in self._params['template']:
      self.log_warn(
          'It seems you are using an unsupported formatting syntax, '
          'please update to the Template Strings syntax: '
          'https://docs.python.org/3/library/string.html#template-strings.')

    # GA4 Measurement Protocol /batch limit is strictly 25
    batch_size = self._params.get('mp_batch_size', 25)
    num_rows = page.num_items
    template = string.Template(self._params['template'])

    # List to hold the array of request bodies
    batch_payloads = []

    # Keys that belong to the Request Body, NOT the Event Parameters
    # We filter these out so they don't accidentally appear inside 'events[].params'
    protocol_keys = {
        'client_id', 'user_id', 'app_instance_id',
        'timestamp_micros', 'non_personalized_ads', 'events'
    }

    for idx, row in enumerate(page):
      try:
        # render the JSON for this specific row
        row_data = json.loads(template.substitute(dict(row.items())))
      except json.decoder.JSONDecodeError as e:
        self.log_warn(f"Skipping row {idx} due to JSON error: {e}")
        continue

      # LOGIC CHECK 1: Determine if 'events' is already structured in the template
      # or if we need to construct it from flat row data.
      if 'events' in row_data:
        event_list = row_data['events']
      else:
        # If no 'events' array exists, treat the remaining data as the event parameters.
        # We assume the template output is flat: {"client_id": "...", "name": "...", ...}
        # We exclude protocol keys to prevent sending 'client_id' inside the event params.
        single_event = {k: v for k, v in row_data.items() if k not in protocol_keys}
        event_list = [single_event]

      # LOGIC CHECK 2: Construct the Request Body (User context)
      request_body = {
          'client_id': row_data.get('client_id'),
          'app_instance_id': row_data.get('app_instance_id'),
          'user_id': row_data.get('user_id'),
          'timestamp_micros': row_data.get('timestamp_micros'),
          'non_personalized_ads': row_data.get('non_personalized_ads'),
          'events': event_list,
      }

      # Remove keys with None values to keep payload clean/minimal
      request_body = {k: v for k, v in request_body.items() if v is not None}

      # Ensure mandatory ID is present (GA4 requires client_id OR app_instance_id)
      if 'client_id' not in request_body and 'app_instance_id' not in request_body:
         self.log_warn(f"Row {idx} missing mandatory 'client_id' or 'app_instance_id'. Skipping.")
         continue

      batch_payloads.append(request_body)

      # Send condition: Batch full OR Last item
      if len(batch_payloads) >= batch_size or (idx + 1) == num_rows:
        if batch_payloads: # Ensure list is not empty
            self._send_payload(batch_payloads, batch_url)
            batch_payloads = [] # Reset for next batch

      # Logging progress
      if idx > 0 and idx % (math.ceil(num_rows / 10)) == 0:
        progress = idx / num_rows
        self.log_info(f'Completed {progress:.2%} of the measurement protocol hits')

    self.log_info('Done with measurement protocol hits.')


  def _execute(self) -> None:
    client = self._get_client()
    dataset = client.get_dataset(
        f'{self._params["bq_project_id"]}.{self._params["bq_dataset_id"]}')
    row_iterator = client.list_rows(
        dataset.table(self._params['bq_table_id']),
        page_token=self._params.get('bq_page_token', None),
        page_size=self._params['bq_batch_size'])

    if self._params['debug']:
      base_url = 'https://www.google-analytics.com/debug/mp/batch'
    else:
      base_url = 'https://www.google-analytics.com/mp/batch'
    # We are only interested in the first page results, since our chunk is
    # fully specicifed by (page_token, batch_size). The next page will be
    # processed by another processing instance.
    first_page = next(row_iterator.pages)
    self._stream_rows(first_page, base_url)
