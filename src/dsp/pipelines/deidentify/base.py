import json
import os
from typing import List, Dict, Union
from requests import Response

import requests_pkcs12
from dsp.shared.aws import secret_value, secret_binary_value, ssm_parameter
from dsp.shared.logger import log_action


class DeidentifyBase:

    def __init__(self, root_domain=''):
        self.root_domain = root_domain

    pod_policy_name = ''
    data_flow_job_name = ''
    ca_cert_path = ''
    p12_cert_path = ''
    aws_p12_path = ''
    config_path = ''
    ppm_prefix = 'policy.'

    root_domain = ''
    p12_secret = ''

    page_size = 200

    @log_action()
    def load_certs(self):
        """Load and writes certificates required to interact with Privitar API"""
        ca_cert = secret_value('/ca/{}/crt'.format(self.get_root_domain()))

        with open(self.ca_cert_path, 'w') as text_file:
            text_file.write(ca_cert)

        p12_cert = secret_binary_value('{}/{}/p12'.format(self.aws_p12_path, self.get_root_domain()))

        with open(self.p12_cert_path, 'wb') as binary_file:
            binary_file.write(p12_cert)

    @log_action()
    def create_json_config(self):
        """Creates configuration for a Deidentify notebook"""
        self.p12_secret = secret_value('{}/{}/password'.format(self.aws_p12_path, self.get_root_domain()))

        config_jobs = self.get_config_jobs()

        data = {
            'secret': self.p12_secret,
            'ppm_fqdn': 'policy.{}'.format(self.get_root_domain())
        }
        data.update(config_jobs)

        with open(self.config_path, 'w') as outfile:
            json.dump(data, outfile)

    def get_root_domain(self) -> str:
        """Gets the root domain for current privitar instance

        Returns:
            str: Domain of the privitar instance.

        """
        if not self.root_domain:
            self.root_domain = ssm_parameter('/core/privitar_root_zone')

        if not self.root_domain.endswith('privacy.data.digital.nhs.uk'):
            raise ValueError('Invalid or missing Privitar root zone')

        return self.root_domain

    def get_p12_secret(self) -> str:
        """Gets the secret required to decrypt the p12 certificate

        Returns:
            str: Secret

        """
        if not self.p12_secret:
            raise ValueError('Invalid or missing Privitar client certificate secret')

        return self.p12_secret

    def get_config_jobs(self) -> Dict:
        """Gets the Priviter Job Ids which should be inserted into the config.

        This is not implemented in the base class.
        Should return a Dictionary with values to insert into the JSON configuration
        for a Deid or GP Data job.

        Returns:
            Dict[str, str]: Key - Deid identify (e.g. 'tokeniseJobId'), Value -
                           Privitar job ID (e.g. 'abc000')

        """
        raise NotImplementedError('This must be implemented by subclass')

    @log_action(log_args=['path'])
    def policy_manager_get(self, path: str):
        """Make a GET request to the Privitar policy manager.

        Args:
            path (str): path to append to base URI.

        Returns:
            requests.Response: Repsonse from policy manager

        """
        headers = {'accept': 'application/json'}

        uri = 'https://{}{}/policy-manager/api/v3/{}'.format(self.ppm_prefix, self.get_root_domain(), path)

        response = requests_pkcs12.get(
            uri, headers=headers, verify=self.ca_cert_path if os.environ.get('env') == 'prod' else False,
            pkcs12_filename=self.p12_cert_path, pkcs12_password=self.get_p12_secret()
        )

        return response

    def get_policy_id(self) -> str:
        """Gets the policy ID from the policy manager.

        Returns:
            str: Policy ID

        """
        response = self.policy_manager_get('policies')

        if response.status_code == 200:
            d = response.json()
            for item in d['items']:
                if item['name'] == self.pod_policy_name:
                    return item['id']
            raise RuntimeError(f'Unable to find policy ID for {self.pod_policy_name}')
        raise ConnectionError(
            f'Bad response from {self.get_root_domain()} with status: {response.status_code}'
        )


    def get_pod_jobs(self, policy_id: str) -> List[Dict[str, str]]:
        """Get all jobs in policy with pagination.

        Args:
            policy_id (str): ID of policy.

        Returns:
            List[Dict[str, str]]: List of Job name pairings. Each item:
                                    'id' : Privitar job ID
                                    'name' : Job name in policy
        """

        def process_response(response_process: Response) -> List[str]:
            if response_process.status_code == 200:
                response_json = response_process.json()
                return [{'id': item['id'], 'name': item['pdd']['name']} for item in response_json['items']]
            else:
                raise Exception(f"The response from the API was not successful with status code: "
                                f"{response_process.status_code} and message {response_process.text}")

        page = 0
        result = []

        response = self.policy_manager_get(f'jobs/pod?policy-id={policy_id}&page-size={self.page_size}&page={page}')
        result.extend(process_response(response))

        while 'nextPage' in response.json():
            page += 1
            response = self.policy_manager_get(f'jobs/pod?policy-id={policy_id}&page-size={self.page_size}&page={page}')
            result.extend(process_response(response))

        return result

    def get_dataflow_job(self) -> Union[str, None]:
        """
        Gets the "dataflow" (transit_id) job id

        Returns:
            str: Job ID of the dataflow job
        """

        response = self.policy_manager_get('jobs/dataflow')

        if response.status_code == 200:
            response_json = response.json()
            for item in response_json['items']:
                if item['name'] == self.data_flow_job_name:
                    return item['id']

        return None

    def get_open_pdds(self) -> List[str]:
        """
        Gets open Protected Data Domains

        Returns:
            str: Protected Data Domain Name
        """
        def process_response(response_process: Response) -> List[str]:
            if response_process.status_code == 200:
                response_json = response_process.json()
                return [item['name'] for item in response_json['items']]
            else:
                raise Exception(f"The response from the API was not successful with status code: "
                                f"{response_process.status_code} and message {response_process.text}")

        page_size = 200
        page = 0
        result = []

        response = self.policy_manager_get(f'pdds?closed=false&page-size={page_size}&page={page}')
        result.extend(process_response(response))

        while 'nextPage' in response.json():
            page += 1
            response = self.policy_manager_get(f'pdds?closed=false&page-size={page_size}&page={page}')
            result.extend(process_response(response))

        return result
