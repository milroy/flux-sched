#!/usr/bin/env python3

import json
import yaml
import random
import boto3
from collections import defaultdict, deque

class Ec2Comm(object):
    """Class to communicate with and receive resources
        from AWS EC2.
    """
    def __init__(self, root=None, jobspec=None):
        self.root = root
        self.jobspec = jobspec
        self.ec2_client = boto3.client('ec2')
        self.ec2_resource = boto3.resource('ec2')
        self.instances = None
        self.core_count = 0
        self.graph = defaultdict(deque)
        self.jgf =''
        self.term = None
        
    def _get_nodecores(self, yml):
        if isinstance(yml, dict):
            for k, v in yml.items():
                if isinstance(v, list) or isinstance(v, dict):
                    yield from self._get_nodecores(v)
        elif isinstance(yml, list):
            for d in yml:
                if d['type'] == 'node':
                    yield 'nodes', d['count']
                elif d['type'] == 'core':
                    yield 'cores', d['count']
                yield from self._get_nodecores(d)

    def set_root (self. root):
        self.root = root
        return 0

    def set_jobspec (self. jobspec):
        self.jobspec = jobspec
        return 0

    def get_jgf (self):
        return self.jgf

    def request_instances(self):
        with open (self.jobspec, 'r') as stream:
            jobspec_dict = yaml.safe_load (stream)
        node_cores = dict(self._get_nodecores(jobspec_dict['resources']))
        self.core_count = node_cores['nodes']
        if node_cores['cores'] > 1:
            print('unsupported node/core config:', node_cores)
            raise NotImplementedError
        else:
            self.instances = self.ec2_resource.create_instances(
                                MinCount=self.core_count, 
                                MaxCount=self.core_count, 
                                UserData='milroy1', 
                                ImageId='ami-03ba3948f6c37a4b0', 
                                InstanceType='t2.micro', 
                                SecurityGroups=['milroy1-lc-flux-dynamism'])
        return

    def ec2_to_jgf(self):
        for inst in self.instances:
            uid = random.getrandbits(64)
            self.graph['nodes'].append({'id': uid,
                              'metadata': {
                                  'name': inst.id,
                                  'uniq_id': uid,
                                  'id': uid,
                                  'exclusive': True,
                                  'type': 'node',
                                  'basename': inst.private_ip_address,
                                  'rank': -1,
                                  'unit': '',
                                  'size': 1,
                                  'paths': {
                                      'containment': self.root + '/' + inst.id
                                  }
                                }
                             })
            self.graph['edges'].append({'source': str(0),
                              'target': str(uid),
                              'metadata': {
                                  'name': {'containment': 'contains'}
                                  }
                               })
            for core in range(inst.cpu_options['CoreCount']):
                cuid = random.getrandbits(64)
                self.graph['nodes'].appendleft({'id': cuid,
                                  'metadata': {
                                      'name': 'core' + str(core),
                                      'uniq_id': cuid,
                                      'id': cuid,
                                      'exclusive': True,
                                      'type': 'core',
                                      'basename': 'ec2-core',
                                      'rank': -1,
                                      'unit': '',
                                      'size': 1,
                                      'paths': {
                                          'containment': self.root + '/' + inst.id
                                            + '/' + 'core' + str(core)
                                      }
                                    }
                                 })
                self.graph['edges'].append({'source': str(uid),
                                  'target': str(cuid),
                                  'metadata': {
                                      'name': {'containment': 'contains'}
                                      }
                                   })
            self.jgf = json.dumps({'graph': {'nodes': list(self.graph['nodes']), 
            'edges': list(self.graph['edges'])}})
            return

    def terminate_instances(self):
        self.term = self.ec2_client.terminate_instances(
                                InstanceIds=[i.id for i in self.instances])
        return
