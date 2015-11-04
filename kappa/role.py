# Copyright (c) 2015 Mitch Garnaat http://garnaat.org/
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

from botocore.exceptions import ClientError

import kappa.aws

LOG = logging.getLogger(__name__)


AssumeRolePolicyDocument = """{
    "Version" : "2012-10-17",
    "Statement": [ {
        "Effect": "Allow",
        "Principal": {
            "Service": [ "lambda.amazonaws.com" ]
        },
        "Action": [ "sts:AssumeRole" ]
    } ]
}"""

LoggingPolicyDocumentTemplate = """{{
  "Version": "2012-10-17",
  "Statement": [
    {{
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:{region}:{account_id}:log-group:/aws/lambda/{function_name}:*"
    }}
  ]
}}"""


class Role(object):

    Path = '/kappa/'

    def __init__(self, context, config):
        self._context = context
        self._config = config
        aws = kappa.aws.get_aws(context)
        self._iam_svc = aws.create_client('iam')
        self._arn = None

    @property
    def name(self):
        return self._config.get('name', self._context.name)

    @property
    def arn(self):
        if self._arn is None:
            try:
                response = self._iam_svc.get_role(
                    RoleName=self.name)
                LOG.debug(response)
                self._arn = response['Role']['Arn']
            except Exception:
                LOG.debug('Unable to find ARN for role: %s', self.name)
        return self._arn

    def _find_all_roles(self):
        # boto3 does not currently do pagination
        # so we have to do it ourselves
        roles = []
        try:
            response = self._iam_svc.list_roles()
            roles += response['Roles']
            while response['IsTruncated']:
                LOG.debug('getting another page of roles')
                response = self._iam_svc.list_roles(
                    Marker=response['Marker'])
                roles += response['Roles']
        except Exception:
            LOG.exception('Error listing roles')
        return roles

    def exists(self):
        for role in self._find_all_roles():
            if role['RoleName'] == self.name:
                return role
        return None

    def create(self):
        role = self.exists()
        if not role:
            LOG.debug('creating role %s', self.name)
            try:
                response = self._iam_svc.create_role(
                    Path=self.Path, RoleName=self.name,
                    AssumeRolePolicyDocument=AssumeRolePolicyDocument)
                LOG.debug(response)

                LOG.debug('attaching logging policy')

                account_id = self._iam_svc.get_user()['User']['Arn'].split(':')[4]

                LOG.debug(str(self._context.lambda_config))
                logging_policy_document = LoggingPolicyDocumentTemplate.format(
                        region=self._iam_svc.meta.region_name,
                        account_id=account_id,
                        function_name=self._context.function.name,
                    )
                
                response = self._iam_svc.put_role_policy(
                        RoleName=self.name,
                        PolicyName='CloudWatchLogs',
                        PolicyDocument=logging_policy_document)
                LOG.debug(response) 
            except ClientError:
                LOG.exception('Error creating Role')
        else:
            LOG.debug('role %s exists', self.name)
        if self._context.policies:
            try:
                for policy in self._context.policies:
                    LOG.debug('attaching policy %s', policy.arn)
                    response = self._iam_svc.attach_role_policy(
                        RoleName=self.name,
                        PolicyArn=policy.arn)
                    LOG.debug(response)
            except ClientError:
                LOG.exception('Error attaching policies')

    def delete(self):
        response = None
        LOG.debug('deleting role %s', self.name)
        try:
            LOG.debug('First detach the policy from the role')
            policy_arns = [policy.arn for policy in self._context.policies]
            for policy_arn in policy_arns:
                if policy_arn:
                    response = self._iam_svc.detach_role_policy(
                        RoleName=self.name, PolicyArn=policy_arn)
                    LOG.debug(response)
            LOG.debug('Now delete role')
            response = self._iam_svc.delete_role(RoleName=self.name)
            LOG.debug(response)
        except ClientError:
            LOG.exception('role %s not found', self.name)
        return response

    def status(self):
        LOG.debug('getting status for role %s', self.name)
        try:
            response = self._iam_svc.get_role(RoleName=self.name)
            LOG.debug(response)
        except ClientError:
            LOG.debug('role %s not found', self.name)
            response = None
        return response
