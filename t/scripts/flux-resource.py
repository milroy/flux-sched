#
#  Run script as `flux resource` with properly configured
#   FLUX_EXEC_PATH or `flux python flux-resource` if not to
#   avoid python version mismatch
#
from __future__ import print_function
import argparse
import errno
import yaml
import json
import flux
import time

def heading ():
    return '{:20} {:20} {:20} {:20}'.format ('JOBID', 'STATUS',
                                             'AT', 'OVERHEAD (Secs)')

def body (jobid, status, at, overhead):
    t = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime (at))
    o = str (overhead)
    return '{:20} {:20} {:20} {:20}'.format (str (jobid), status, t, o)

def width ():
    return 20 + 20 + 20 + 20

"""
    Class to interface with the resource module with RPC
"""
class ResourceModuleInterface:
    def __init__ (self):
        self.f = flux.Flux ()
        self.my_uri = self.f.attr_get ("local-uri")
        try:
            from ec2api import Ec2Comm
            self.ec2comm = Ec2Comm. ()
        except ImportError:
            self.ec2comm = None
        try:
            self.jobid = self.f.attr_get ("jobid")
        except:
            self.jobid = 0
        try:
            self.parent_uri = self.f.attr_get ("parent-uri")
            parent_handle = flux.Flux(url=parent_uri)
            success = parent_handle.attr_set(
                "child-uri" + "-" + str(self.jobid), self.my_uri)
        except:
            self.parent_uri = None
    

    def rpc_next_jobid (self):
        resp = self.f.rpc ("resource.next_jobid").get ()
        return resp['jobid']

    def rpc_allocate (self, jobid, jobspec_str):
        payload = {'cmd' : 'allocate', 'jobid' : jobid, 'jobspec' : jobspec_str}
        return self.f.rpc ("resource.match", payload).get ()

    def rpc_allocate_with_satisfiability (self, jobid, jobspec_str):
        payload = {'cmd' : 'allocate_with_satisfiability',
                   'jobid' : jobid, 'jobspec' : jobspec_str}
        return self.f.rpc ("resource.match", payload).get ()

    def rpc_reserve (self, jobid, jobspec_str):
        payload = {'cmd' : 'allocate_orelse_reserve',
                   'jobid' : jobid, 'jobspec' : jobspec_str}
        return self.f.rpc ("resource.match", payload).get ()

    def rpc_grow (self, jobid, subgraph_str):
        payload = {'jobid' : jobid, 'subgraph' : subgraph_str}
        return self.f.rpc ("resource.grow", payload).get ()

    def rpc_match_grow (self, jobid, jobspec_str):
        jid = 0
        if jobid != 0:
            jid = jobid
        payload = {'cmd' : 'grow', 'jobid' : jid, 'jobspec' : jobspec_str}
        return self.f.rpc ("resource.match", payload).get ()

    def rpc_shrink (self, path, jobid, detach, up):
        payload = {'path' : path, 'jobid' : jobid, 'detach': detach, 'up' : up}
        return self.f.rpc ("resource.shrink", payload).get ()

    def rpc_detach (self, path, jobid, subgraph, up):
        payload = {'path' : path, 'jobid' : jobid, 'subgraph': subgraph, 'up' : up}
        return self.f.rpc ("resource.detach", payload).get ()

    def rpc_create_ec2_resources (self, root, jobspec_str):
        if not self.ec2comm:
            payload = {'subgraph': ''}
        else:
            self.ec2comm.root = root
            self.ec2comm.jobspec = jobspec_str
            self.ec2comm.request_instances ()
            self.ec2comm.ec2_to_jgf ()
            payload = {'subgraph': self.ec2comm.jgf}
        return self.f.rpc ("resource.ec2_create", payload).get ()

    def rpc_info (self, jobid):
        payload = {'jobid' : jobid}
        return self.f.rpc ("resource.info", payload).get ()

    def rpc_stat (self):
        return self.f.rpc ("resource.stat").get ()

    def rpc_cancel (self, jobid):
        payload = {'jobid' : jobid}
        return self.f.rpc ("resource.cancel", payload).get ()
   
    def rpc_set_property (self, sp_resource_path, sp_keyval):
        payload = {'sp_resource_path' : sp_resource_path, 
            'sp_keyval' : sp_keyval}
        return self.f.rpc ("resource.set_property", payload).get ()
    
    def rpc_get_property (self, gp_resource_path, gp_key):
        payload = {'gp_resource_path' : gp_resource_path, 'gp_key' : gp_key}
        return self.f.rpc ("resource.get_property", payload).get ()
    

"""
    Action for match allocate sub-command
"""
def match_alloc_action (args):
    with open (args.jobspec, 'r') as stream:
        jobspec_str = yaml.dump (yaml.load (stream))
        r = ResourceModuleInterface ()
        resp = r.rpc_allocate (r.rpc_next_jobid (), jobspec_str)
        print (heading ())
        print (body (resp['jobid'], resp['status'], resp['at'], resp['overhead']))
        print ("=" * width ())
        print ("MATCHED RESOURCES:")
        print (resp['R'])
"""
    Action for match grow sub-command
"""
def match_grow_action (args):
    with open (args.jobspec, 'r') as stream:
        jobspec_str = yaml.dump (yaml.load (stream))
        r = ResourceModuleInterface ()
        resp = r.rpc_match_grow (args.jobid, jobspec_str)
        print (heading ())
        print (body (resp['jobid'], resp['status'], resp['at'], resp['overhead']))
        print ("=" * width ())
        print ("MATCHED RESOURCES:")
        print (resp['R'])

"""
    Action for match allocate_with_satisfiability sub-command
"""
def match_alloc_sat_action (args):
    with open (args.jobspec, 'r') as stream:
        jobspec_str = yaml.dump (yaml.load (stream))
        r = ResourceModuleInterface ()
        resp = r.rpc_allocate_with_satisfiability (r.rpc_next_jobid (),
                                                   jobspec_str)
        print (heading ())
        print (body (resp['jobid'], resp['status'], resp['at'], resp['overhead']))
        print ("=" * width ())
        print ("MATCHED RESOURCES:")
        print (resp['R'])

"""
    Action for match allocate_orelse_reserve sub-command
"""
def match_reserve_action (args):
    with open (args.jobspec, 'r') as stream:
        jobspec_str = yaml.dump (yaml.load (stream))
        r = ResourceModuleInterface ()
        resp = r.rpc_reserve (r.rpc_next_jobid (), jobspec_str)
        print (heading ())
        print (body (resp['jobid'], resp['status'], resp['at'], resp['overhead']))
        print ("=" * width ())
        print ("MATCHED RESOURCES:")
        print (resp['R'])

"""
    Action for grow command
"""
def grow_action (args):
    with open (args.subgraph, 'r') as stream:
        subgraph_str = json.dumps (json.loads (stream))
        r = ResourceModuleInterface ()
        resp = r.rpc_grow (args.jobid, subgraph_str)
        print (resp['result'])

"""
    Action for shrink sub-command
"""
def shrink_action (args):
    r = ResourceModuleInterface ()
    detach = args.detach.lower ()
    up = args.up.lower ()
    bdetach = False
    bup = True
    if detach == 'true':
        bdetach = True
    else:
        bdetach = False
    if up == 'true':
        bup = True
    else:
        bup = False
    resp = r.rpc_shrink (args.path, args.jobid, bdetach, bup)
    print (resp['result'])


"""
    Action for detach sub-command
"""
def detach_action (args):
    with open (args.subgraph, 'r') as stream:
        subgraph = json.dumps (json.loads (stream))
        r = ResourceModuleInterface ()
        up = args.up.lower ()
        bup = True
        if up == 'true':
            bup = True
        else:
            bup = False
        resp = r.rpc_detach (args.path, args.jobid, subgraph, bup)
        print (resp['result'])

"""
    Action to create EC2 resources
"""
def create_ec2_resources_action (args):
    with open (args.jobspec, 'r') as stream:
        jobspec_str = json.dumps (json.loads (stream))
        r = ResourceModuleInterface ()
        resp = r.rpc_create_ec2_resources (args.root, args.jobspec)
        print (resp['subgraph'])

"""
    Action for cancel sub-command
"""
def cancel_action (args):
    r = ResourceModuleInterface ()
    jobid = args.jobid
    resp = r.rpc_cancel (jobid)

"""
    Action for info sub-command
"""
def info_action (args):
    r = ResourceModuleInterface ()
    jobid = args.jobid
    resp = r.rpc_info (jobid)
    print (heading ())
    print (body (resp['jobid'], resp['status'], resp['at'], resp['overhead']))

"""
    Action for stat sub-command
"""
def stat_action (args):
    r = ResourceModuleInterface ()
    resp = r.rpc_stat ()
    print ("Num. of Vertices: ", resp['V'])
    print ("Num. of Edges: ", resp['E'])
    print ("Graph Load Time: ", resp['load-time'], "Secs")
    print ("Num. of Jobs Matched: ", resp['njobs'])
    print ("Min. Match Time: ", resp['min-match'], "Secs")
    print ("Max. Match Time: ", resp['max-match'], "Secs")
    print ("Avg. Match Time: ", resp['avg-match'], "Secs")

"""
    Action for set-property sub-command
"""
def set_property_action (args):
    r = ResourceModuleInterface ()
    sp_resource_path = args.sp_resource_path
    sp_keyval = args.sp_keyval
    resp = r.rpc_set_property (sp_resource_path, sp_keyval)

"""
    Action for get-property sub-command
"""
def get_property_action (args):
    r = ResourceModuleInterface ()
    gp_resource_path = args.gp_resource_path
    gp_key = args.gp_key
    resp = r.rpc_get_property (gp_resource_path, gp_key)
    print (args.gp_key, "=", resp['value'])

"""
    Main entry point
"""
def main ():
    #
    # Main command arguments/options
    #
    desc = 'Front-end command for resource '\
           'module for testing. Provide 4 sub-commands. '\
           'For sub-command usage, '\
           '%(prog)s <sub-command> --help'
    parser = argparse.ArgumentParser (description=desc)
    parser.add_argument ('-v', '--verbose', action='store_true',
                         help='be verbose')

    #
    # Add subparser for the top-level sub-commands
    #
    subpar = parser.add_subparsers (title='Available Commands',
                                    description='Valid commands',
                                    help='Additional help')
    mstr = "Find the best matching resources for a jobspec"
    istr = "Print info on a single job"
    sstr = "Print overall performance statistics"
    cstr = "Cancel an allocated or reserved job"
    pstr = "Set property-key=value for specified resource."
    gstr = "Get value for specified resource and property-key."
    shstr = "Shrink an allocated job"
    dtstr = "Detach subgraph from resource graph"
    grstr = "Grow resource graph with subgraph"
    crec2str = "Create resources in EC2 from jobspec"
    parser_m = subpar.add_parser ('match', help=mstr, description=mstr)
    parser_i = subpar.add_parser ('info', help=istr, description=istr)
    parser_s = subpar.add_parser ('stat', help=sstr, description=sstr)
    parser_c = subpar.add_parser ('cancel', help=cstr, description=cstr)
    parser_sp = subpar.add_parser ('set-property', help=pstr, description=pstr)
    parser_gp = subpar.add_parser ('get-property', help=gstr, description=gstr)
    parser_sh = subpar.add_parser ('shrink', help=shstr, description=shstr)
    parser_dt = subpar.add_parser ('detach', help=dtstr, description=dtstr)
    parser_gr = subpar.add_parser ('grow', help=grstr, description=grstr)
    parser_crec2= subpar.add_parser ('ec2-create', help=crec2str, description=crec2str)

    #
    # Add subparser for the match sub-command
    #
    subparsers_m = parser_m.add_subparsers (title='Available Commands',
                                           description='Valid commands',
                                           help='Additional help')

    mastr = "Allocate the best matching resources if found"
    mgstr = "Allocate the best matching resources if found, "\
            "and grow my job with the matching resources."
    msstr = "Allocate the best matching resources if found. "\
            "If not found, check jobspec's overall satisfiability"
    mrstr = "Allocate the best matching resources if found. "\
            "If not found, reserve them instead at earliest time"
    parser_ma = subparsers_m.add_parser ('allocate', help=mastr)
    parser_mg = subparsers_m.add_parser ('grow', help=mgstr)
    parser_ms = subparsers_m.add_parser ('allocate_with_satisfiability',
                                          help=msstr)
    parser_mr = subparsers_m.add_parser ('allocate_orelse_reserve', help=mrstr)

    #
    # Positional argument for info sub-command
    #
    parser_i.add_argument ('jobid', metavar='Jobid', type=int, help='Jobid')
    parser_i.set_defaults (func=info_action)

    #
    # Action for stat sub-command
    #
    parser_s.set_defaults (func=stat_action)

    #
    # Positional argument for cancel sub-command
    #
    parser_c.add_argument ('jobid', metavar='Jobid', type=int, help='Jobid')
    parser_c.set_defaults (func=cancel_action)

    #
    # Positional argument for match allocate sub-command
    #
    parser_ma.add_argument ('jobspec', metavar='Jobspec', type=str,
                            help='Jobspec file name')
    parser_ma.set_defaults (func=match_alloc_action)

    #
    # Positional argument for match grow sub-command
    #
    parser_mg.add_argument ('jobspec', metavar='Jobspec', type=str,
                            help='Jobspec file name')
    parser_mg.set_defaults (func=match_grow_action)

    #
    # Positional argument for match allocate_with_satisfiability sub-command
    #
    parser_ms.add_argument ('jobspec', metavar='Jobspec', type=str,
                            help='Jobspec file name')
    parser_ms.set_defaults (func=match_alloc_sat_action)

    #
    # Positional argument for match allocate_orelse_reserve sub-command
    #
    parser_mr.add_argument ('jobspec', metavar='Jobspec', type=str,
                            help='Jobspec file name')
    parser_mr.set_defaults (func=match_reserve_action)

    # Positional arguments for set-property sub-command
    #
    parser_sp.add_argument ('sp_resource_path', metavar='ResourcePath', 
            type=str, help='set-property resource_path property-key=val')
    parser_sp.add_argument ('sp_keyval', metavar='PropertyKeyVal', type=str,
                            help='set-property resource_path property-key=val')
    parser_sp.set_defaults (func=set_property_action)

    # Positional argument for get-property sub-command
    #
    parser_gp.add_argument ('gp_resource_path', metavar='ResourcePath', 
                type=str, help='get-property resource_path property-key')
    parser_gp.add_argument ('gp_key', metavar='PropertyKey', type=str,
                            help='get-property resource_path property-key')
    parser_gp.set_defaults (func=get_property_action)

    # Positional arguments for shrink command
    #
    parser_sh.add_argument ('path', metavar='ShrinkPath', 
                type=str, help='shrink path')
    parser_sh.add_argument ('jobid', metavar='JobID', type=int,
                            help='job id to shrink')
    parser_sh.add_argument ('detach', metavar='Detach', type=str,
                            help='delete from resource graph?')
    parser_sh.add_argument ('up', metavar='Up', type=str,
                            help='traverse graph upward?')
    parser_sh.set_defaults (func=shrink_action)

    # Positional arguments for detach command
    #
    parser_dt.add_argument ('path', metavar='ShrinkPath', 
                type=str, help='shrink path')
    parser_dt.add_argument ('jobid', metavar='JobID', type=int,
                            help='job id to shrink')
    parser_dt.add_argument ('subgraph', metavar='Subgraph', type=str,
                            help='subgraph to delete')
    parser_dt.add_argument ('up', metavar='Up', type=str,
                            help='traverse graph upward?')
    parser_dt.set_defaults (func=detach_action)

    # Positional arguments for grow command
    #
    parser_gr.add_argument ('jobid', metavar='JobID', type=int,
                            help='job id to grow')
    parser_gr.add_argument ('subgraph', metavar='Subgraph', type=str,
                            help='subgraph to attach')
    parser_gr.set_defaults (func=grow_action)

    # Positional arguments to create EC2 resources
    #
    parser_crec2.add_argument ('root', metavar='Root', type=int,
                            help='cluster root to attach EC2 vertices')
    parser_crec2.add_argument ('jobspec', metavar='Jobspec', type=str,
                            help='jobspec to match in EC2')
    parser_crec2.set_defaults (func=create_ec2_resources_action)

    #
    # Parse the args and call an action routine as part of that
    #
    try:
        args = parser.parse_args ()
        args.func (args)

    except (IOError,EnvironmentError) as e:
        name = e.__class__.__name__
        print ("{}: error({}): {}".format (name, e.errno, e.strerror))
        if e.errno == errno.ENOENT:
            exit (3)
        if e.errno == errno.EBUSY:  # resource currently unavailable
            exit (16)
        if e.errno == errno.ENODEV: # unsatisfiable jobspec
            exit (19)
        else:
            exit (1)

    except yaml.YAMLError as e:
        print ("Parsing error: ", e)
        exit (2)

if __name__ == "__main__":
    main ()

#
# vi:tabstop=4 shiftwidth=4 expandtab
#
