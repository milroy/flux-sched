#!/usr/bin/env python3

import json
import yaml
import random
import boto3
import re
from collections import defaultdict, deque

class Ec2Comm(object):
    """Class to communicate with and receive resources
        from AWS EC2.
    """
    def __init__(self, root=None, jobspec=None):
        self.root = root
        self.jobspec = [jobspec]
        self.ec2_client = boto3.client('ec2')
        self.ec2_resource = boto3.resource('ec2')
        self.instances = {} 
        self.core_count = []
        self.graph = []
        self.jgf = []
        self.term = None
        self.latest_inst = None
        
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

    def set_root (self, root):
        print (root)
        self.root = root
        return 0

    def set_jobspec (self, jobspec):
        print (jobspec)
        self.jobspec.append(jobspec)
        return 0

    def get_jgf (self):
        return self.jgf[-1]

    def request_instances(self):
        try:
            jobspec_dict = yaml.safe_load (self.jobspec[-1])
        except:
            print ("can't get file")
            return
        node_cores = dict(self._get_nodecores(jobspec_dict['resources']))
        self.core_count = node_cores['nodes']
        if node_cores['cores'] > 1:
            print('unsupported node/core config:', node_cores)
            raise NotImplementedError
        else:
            self.latest_inst =  self.ec2_resource.create_instances(
                                MinCount=self.core_count, 
                                MaxCount=self.core_count, 
                                UserData='milroy1', 
                                ImageId='ami-03ba3948f6c37a4b0', 
                                InstanceType='t2.micro', 
                                SecurityGroups=['milroy1-lc-flux-dynamism'])
        return

    def ec2_to_jgf(self):
        subgraph = defaultdict(deque)
        for inst in self.latest_inst:
            self.instances[inst.id] = inst
            uid = random.getrandbits(62)
            subgraph['nodes'].append({'id': str(uid),
                              'metadata': {
                                  'type': 'node',
                                  'basename': inst.private_ip_address,
                                  'name': inst.id,
                                  'id': uid,
                                  'uniq_id': uid,
                                  'rank': -1,
                                  'exclusive': True,                  
                                  'unit': '',
                                  'size': 1,
                                  'paths': {
                                      'containment': '/' + self.root + 
                                      '/' + inst.id
                                  }
                                }
                             })
            subgraph['edges'].append({'source': str(0),
                              'target': str(uid),
                              'metadata': {
                                  'name': {'containment': 'contains'}
                                  }
                               })
            for core in range(1): # must be changed back to 
                #inst.cpu_options['CoreCount']), but interface is messed up
                cuid = random.getrandbits(62)
                subgraph['nodes'].appendleft({'id': str(cuid),
                                  'metadata': {
                                      'type': 'core',
                                      'basename': 'ec2-core',
                                      'name': 'core' + str(core),
                                      'id': cuid,
                                      'uniq_id': cuid,
                                      'rank': -1,
                                      'exclusive': True,
                                      'unit': '',
                                      'size': 1,
                                      'paths': {
                                          'containment': '/' + self.root + 
                                          '/' + inst.id + '/' + 'core' + 
                                          str(core)
                                      }
                                    }
                                 })
                subgraph['edges'].append({'source': str(uid),
                                  'target': str(cuid),
                                  'metadata': {
                                      'name': {'containment': 'contains'}
                                      }
                                   })
        subgraph['nodes'].append({'id': '0',
                  'metadata': {
                      'type': 'cluster',
                      'basename': re.sub(r'\d+','', self.root),
                      'name': self.root,
                      'id': 0,
                      'uniq_id': 0,
                      'rank': -1,
                      'exclusive': False,                  
                      'unit': '',
                      'size': 1,
                      'paths': {
                          'containment': '/' + self.root
                      }
                    }
                 })
        print (self.instances)
        self.graph.append(subgraph)
        self.jgf.append(json.dumps({'graph': {'nodes': list(subgraph['nodes']), 
            'edges': list(subgraph['edges'])}}))
        return

    def terminate_instances(self):
       # self.term = self.ec2_client.terminate_instances(
       #                         InstanceIds=[i.id for i in self.instances])
        return
