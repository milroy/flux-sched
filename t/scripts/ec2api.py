#!/usr/bin/env python3

import json
import yaml
import random
import boto3
import re
from collections import defaultdict, deque
import time

ec2_types = {'g2.2xlarge': (8, 15, 1),
            'g3.4xlarge': (16, 128, 4),
            't2.micro': (1, 1, 0),
            't2.small': (1, 2, 0),
            't2.medium': (2, 4, 0),
            't2.large': (2, 8, 0),
            't2.xlarge': (4, 16, 0),
            't2.2xlarge': (8, 32, 0)  
            }

res_to_ec2 = {frozenset(['cores', 'memory', 'gpu']): (
                                                ('g2.2xlarge', (8, 15, 1)),
                                                ('g3.4xlarge', (16, 128, 4))
                                                )}

res_to_ec2[frozenset(['cores', 'memory'])] = (
                                            ('t2.micro', (1, 1, 0)),
                                            ('t2.small', (1, 2, 0)),
                                            ('t2.medium', (2, 4, 0)),
                                            ('t2.large', (2, 8, 0)),
                                            ('t2.xlarge', (4, 16, 0)),
                                            ('t2.2xlarge', (8, 32, 0))
                                           )

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
        self.node_list = []
        self.graph = []
        self.jgf = []
        self.term = None
        self.latest_inst = []
        
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
                elif d['type'] == 'memory':
                    yield 'memory', d['count']
                elif d['type'] == 'gpu':
                    yield 'gpu', d['count']
                yield from self._get_nodecores(d)

    def set_root (self, root):
        self.root = root
        return 0

    def set_jobspec (self, jobspec):
        self.jobspec.append(jobspec)
        return 0

    def get_jgf (self):
        return self.jgf[-1]

    def _set_nodelist (self, js_dict):
        self.node_list = []
        for k, v in self._get_nodecores(js_dict['resources']):
            if k == 'nodes':
                self.node_list.append({'nodes': v})
            else:
                self.node_list[-1][k] = v

    def map_to_ec2 (self):
        nmap = defaultdict(int)
        for n in self.node_list:
            amap = [0, 0, 0]
            iset = set(['cores', 'memory'])
            amap[0] = n['cores']
            if 'gpu' in n:
                amap[2] = n['gpu']
                iset.add('gpu')
            if 'memory' in n:
                amap[1] = n['memory']
            for et in res_to_ec2[frozenset(iset)]:
                if all(x >= y for x, y in zip(et[1], amap)):
                    nmap[et[0]] += n['nodes']
                    break
        return nmap

    def request_instances (self):
        try:
            jobspec_dict = yaml.safe_load (self.jobspec[-1])
        except:
            print ("can't convert jobspec to dict")
            return

        self._set_nodelist (jobspec_dict)
        request = self.map_to_ec2 ()
        if not request:
            print('unsupported node/core config:', jobspec_dict['resources'])
            raise NotImplementedError
        else:
            self.latest_inst = []
            start = time.perf_counter ()
            for ec2type, count in request.items ():
                self.latest_inst.append (self.ec2_resource.create_instances(
                                        MinCount=count, 
                                        MaxCount=count, 
                                        UserData='milroy1', 
                                        ImageId='ami-03ba3948f6c37a4b0', 
                                        InstanceType=ec2type, 
                                        SecurityGroups=['milroy1-lc-flux-dynamism'])
                                        )

            print ('time to create EC2 instances:', time.perf_counter () - start)

            for inst in self.latest_inst:
                for i in inst:
                    self.instances[i.id] = i
            print (self.instances)
        return

    def ec2_to_jgf(self):
        subgraph = defaultdict(deque)
        for inst_type in self.latest_inst:
            for inst in inst_type:
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
                for core in range(inst.cpu_options['CoreCount']):
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
                for mem in range(ec2_types[inst.instance_type][1]):
                    muid = random.getrandbits(62)
                    subgraph['nodes'].appendleft({'id': str(muid),
                                      'metadata': {
                                          'type': 'memory',
                                          'basename': 'ec2-memory',
                                          'name': 'memory' + str(mem),
                                          'id': muid,
                                          'uniq_id': muid,
                                          'rank': -1,
                                          'exclusive': True,
                                          'unit': '',
                                          'size': 1,
                                          'paths': {
                                              'containment': '/' + self.root + 
                                              '/' + inst.id + '/' + 'memory' + 
                                              str(mem)
                                          }
                                        }
                                     })
                    subgraph['edges'].append({'source': str(uid),
                                      'target': str(muid),
                                      'metadata': {
                                          'name': {'containment': 'contains'}
                                          }
                                       })
                for gpu in range(ec2_types[inst.instance_type][2]):
                    gpuid = random.getrandbits(62)
                    subgraph['nodes'].appendleft({'id': str(gpuid),
                                      'metadata': {
                                          'type': 'gpu',
                                          'basename': 'ec2-gpu',
                                          'name': 'gpu' + str(gpu),
                                          'id': gpuid,
                                          'uniq_id': gpuid,
                                          'rank': -1,
                                          'exclusive': True,
                                          'unit': '',
                                          'size': 1,
                                          'paths': {
                                              'containment': '/' + self.root + 
                                              '/' + inst.id + '/' + 'gpu' + 
                                              str(gpu)
                                          }
                                        }
                                     })
                    subgraph['edges'].append({'source': str(uid),
                                      'target': str(gpuid),
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
        self.graph.append(subgraph)
        self.jgf.append(json.dumps({'graph': {'nodes': list(subgraph['nodes']), 
            'edges': list(subgraph['edges'])}}))
        return

    def terminate_instances(self):
        if self.instances:
             self.term = self.ec2_client.terminate_instances(
                                 InstanceIds=[v.id for k, v in \
                                              self.instances.items ()])
        return
