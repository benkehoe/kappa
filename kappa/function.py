# Copyright (c) 2014 Mitch Garnaat http://garnaat.org/
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import logging
import os
import zipfile
import time
import json

from botocore.exceptions import ClientError

import kappa.aws
import kappa.log

LOG = logging.getLogger(__name__)


class Function(object):
    DEFAULT_MEMORY = int(os.getenv('KAPPA_DEFAULT_TIMEOUT', '128'))
    DEFAULT_TIMEOUT = int(os.getenv('KAPPA_DEFAULT_TIMEOUT', '3'))

    def __init__(self, context, config):
        self._context = context
        self._config = config
        aws = kappa.aws.get_aws(context)
        self._lambda_svc = aws.create_client('lambda')
        self._s3_svc = aws.create_client('s3')
        self._arn = None
        self._log = None

    @property
    def name(self):
        return self._config.get('name', self._context.name)

    @property
    def runtime(self):
        if 'runtime' not in self._config:
            runtime = self._context.get_default_runtime()
            if runtime:
                return runtime
        return self._config['runtime']

    @property
    def handler(self):
        return self._config['handler']

    @property
    def description(self):
        return self._config.get('description', '')

    @property
    def timeout(self):
        return self._config.get('timeout', self.DEFAULT_TIMEOUT)

    @property
    def memory_size(self):
        return self._config.get('memory_size', self.DEFAULT_MEMORY)

    @property
    def s3(self):
        return self._config.get('s3', None)

    @property
    def s3_only(self):
        return self._config.get('s3', {}).get('only', False)

    @property
    def zipfile_name(self):
        return self._config.get('zipfile_name', self._context.name + '.zip')

    @property
    def path(self):
        return self._config.get('path', 'src/')

    @property
    def test_data(self):
        return self._config.get('test_data')

    @property
    def permissions(self):
        return self._config.get('permissions', list())

    @property
    def arn(self):
        if self._arn is None:
            try:
                response = self._lambda_svc.get_function(
                    FunctionName=self.name)
                LOG.debug(response)
                self._arn = response['Configuration']['FunctionArn']
            except Exception:
                LOG.debug('Unable to find ARN for function: %s', self.name)
        return self._arn

    @property
    def log(self):
        if self._log is None:
            log_group_name = '/aws/lambda/%s' % self.name
            self._log = kappa.log.Log(self._context, log_group_name)
        return self._log

    def exists(self):
        return bool(self.arn)

    def tail(self):
        LOG.debug('tailing function: %s', self.name)
        return self.log.tail()

    def _zip_lambda_dir(self, zipfile_name, lambda_dir):
        LOG.debug('_zip_lambda_dir: lambda_dir=%s', lambda_dir)
        LOG.debug('zipfile_name=%s', zipfile_name)
        relroot = os.path.abspath(lambda_dir)
        with zipfile.ZipFile(zipfile_name, 'w',
                             compression=zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(lambda_dir):
                zf.write(root, os.path.relpath(root, relroot))
                for filename in files:
                    filepath = os.path.join(root, filename)
                    if os.path.isfile(filepath):
                        arcname = os.path.join(
                            os.path.relpath(root, relroot), filename)
                        zf.write(filepath, arcname)

    def _zip_lambda_file(self, zipfile_name, lambda_file):
        LOG.debug('_zip_lambda_file: lambda_file=%s', lambda_file)
        LOG.debug('zipfile_name=%s', zipfile_name)
        with zipfile.ZipFile(zipfile_name, 'w',
                             compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(lambda_file)

    def zip_lambda_function(self, zipfile_name, lambda_fn):
        if os.path.isdir(lambda_fn):
            self._zip_lambda_dir(zipfile_name, lambda_fn)
        else:
            self._zip_lambda_file(zipfile_name, lambda_fn)

    def add_permissions(self):
        for permission in self.permissions:
            try:
                kwargs = {
                    'FunctionName': self.name,
                    'StatementId': permission['statement_id'],
                    'Action': permission['action'],
                    'Principal': permission['principal']}
                source_arn = permission.get('source_arn', None)
                if source_arn:
                    kwargs['SourceArn'] = source_arn
                source_account = permission.get('source_account', None)
                if source_account:
                    kwargs['SourceAccount'] = source_account
                response = self._lambda_svc.add_permission(**kwargs)
                LOG.debug(response)
            except Exception:
                LOG.exception('Unable to add permission')

    def create(self):
        LOG.debug('creating %s', self.zipfile_name)
        self.zip_lambda_function(self.zipfile_name, self.path)
        with open(self.zipfile_name, 'rb') as fp:
            exec_role = self._context.exec_role_arn
            LOG.debug('exec_role=%s', exec_role)
            
            zipdata = fp.read()
            if self.s3:
                bucket = self.s3['bucket']
                key = self.s3.get('key', self.name)

                try:
                    LOG.info('uploading to s3://%s/%s', bucket, key)
                    response = self._s3_svc.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=zipdata,
                        ContentType='application/zip')
                    LOG.debug(response)
                    code = {'S3Bucket': bucket, 'S3Key': key}
                except Exception:
                    LOG.exception('Unable to upload zip file')
                    return
            else:
                code = {'ZipFile': zipdata}
            
            if not self.s3_only:
                try:
                    LOG.debug('Creating function')
                    response = self._lambda_svc.create_function(
                        FunctionName=self.name,
                        Code=code,
                        Runtime=self.runtime,
                        Role=exec_role,
                        Handler=self.handler,
                        Description=self.description,
                        Timeout=self.timeout,
                        MemorySize=self.memory_size)
                    LOG.debug(response)
                except Exception:
                    LOG.exception('Unable to create function')
        self.add_permissions()

    def deploy(self):
        if self.exists():
            return self.update()
        else:
            return self.create()

    def update(self):
        LOG.debug('updating %s', self.zipfile_name)
        self.zip_lambda_function(self.zipfile_name, self.path)
        with open(self.zipfile_name, 'rb') as fp:
            try:
                LOG.debug('updating code')
                zipdata = fp.read()
                response = self._lambda_svc.update_function_code(
                    FunctionName=self.name,
                    ZipFile=zipdata)
                LOG.debug(response)
                
                LOG.debug('updating configuration')
                response = self._lambda_svc.update_function_configuration(
                    FunctionName=self.name,
                    Role=self._context.exec_role_arn,
                    Handler=self.handler,
                    Description=self.description,
                    Timeout=self.timeout,
                    MemorySize=self.memory_size)
                LOG.debug(response)
            except Exception:
                LOG.exception('Unable to update zip file')

    def delete(self):
        LOG.debug('deleting function %s', self.name)
        response = None
        try:
            response = self._lambda_svc.delete_function(FunctionName=self.name)
            LOG.debug(response)
        except ClientError:
            LOG.debug('function %s: not found', self.name)
        return response

    def status(self):
        LOG.debug('getting status for function %s', self.name)
        try:
            response = self._lambda_svc.get_function(
                FunctionName=self.name)
            LOG.debug(response)
        except ClientError:
            LOG.debug('function %s not found', self.name)
            response = None
        return response

    def _get_test_data(self, test_data):
        if test_data is None:
            if not self.test_data:
                test_data="null"
            else:
                with open(self.test_data) as fp:
                    test_data = fp.read()
        return test_data

    def _invoke(self, test_data, invocation_type):
        test_data = self._get_test_data(test_data)
        LOG.debug('invoke %s', test_data)
        response = self._lambda_svc.invoke(
            FunctionName=self.name,
            InvocationType=invocation_type,
            LogType='Tail',
            Payload=test_data)
        LOG.debug(response)
        return response

    def invoke(self, test_data=None, dry_run=False):
        if dry_run:
            return self._invoke(test_data, 'DryRun')
        else:
            return self._invoke(test_data, 'RequestResponse')

    def invoke_async(self, test_data=None):
        return self._invoke(test_data, 'Event')

    def invoke_local(self, test_data=None):
        import sys
        sys.path.insert(0, self.path)
        module_name = '.'.join(self.handler.split('.')[:-1])
        func_name = self.handler.split('.')[-1]
        import importlib
        module = importlib.import_module(module_name)
        func = getattr(module, func_name)

        test_data = self._get_test_data(test_data)
        try:
            event = json.loads(test_data)
        except:
            event = test_data
        context = _FakeLambdaContext(
            function_name=self.name,
            memory_size=self.memory_size,
            timeout=self.timeout)

        return func(event, context)

class _FakeLambdaContext(object):
    def __init__(self,
            function_name='FunctionName',
            function_version='$LATEST',
            memory_size=128,
            timeout=3,
            start=None):
        import time, uuid
        
        if start is None:
            start = time.time()

        self._timeout = timeout
        self._start = start
        self._get_time = time.time

        self.function_name = function_name
        self.function_version = function_version
        self.invoked_function_arn = 'arn:aws:lambda:us-east-1:000000000000:function:{}:{}'.format(self.function_name, self.function_version)
        self.memory_limit_in_mb = memory_size
        self.aws_request_id = uuid.uuid4()
        self.log_group_name = '/aws/lambda/{}'.format(self.function_name)
        self.log_stream_name = '{}/[{}]{}'.format(
            time.strftime('%Y/%m/%d', time.gmtime(self._start)),
            self.function_version,
            self.aws_request_id)
        self.identity = None
        self.client_context = None

    def get_remaining_time_in_millis(self):
        time_used = self._get_time()- self._start
        time_left = self._timeout * 1000 - time_used
        return int(round(time_left * 1000))
