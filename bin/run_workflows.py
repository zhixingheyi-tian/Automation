from utils.run_workflow_utils import *
from utils.setup_cluster import *

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument('--workflows', type=str, default="")
    parser.add_argument('--plugins', type=str, default=None)
    parser.add_argument('--deploy', type=bool, default=False)
    args = parser.parse_args()

    plugins = args.plugins.strip().strip("\n").split(",")
    workflows = args.workflows.strip().strip("\n").split(",")
    if len(workflows) == 0:
        print "Please input right workflow"
        exit(1)
    if args.deploy:
        testing_conf_list = []
        dataGen_conf_list = []
        worflow_for_deploy= os.path.abspath(workflows[0])
        update_workflow(worflow_for_deploy)
        output_workflow = os.path.join(worflow_for_deploy, "output/output_workflow")
        get_conf_list(output_workflow, testing_conf_list, dataGen_conf_list)
        # setup_cluster(testing_conf_list[0])
        TPCDS.deploy(testing_conf_list[0] ,plugins)

    for workflow in workflows:
        run_workflow(os.path.abspath(workflow), plugins)


