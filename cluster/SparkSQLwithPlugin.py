from cluster.SparkSQL import *

def spark_sql_compile_with_plugin(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    for plugin in plugins:
        if plugin == "oap":
            oap_compile(beaver_env, master)
        if plugin == "conda_oap":
            conda_oap_compile(beaver_env, master)
    spark_sql_source_code_compile(custom_conf)

def deploy_spark_sql_with_plugin(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    # Deploy SparkSQL
    deploy_spark_sql(custom_conf)
    for plugin in plugins:
        if plugin == "oap":
            # Deploy OAP
            deploy_oap_internal(custom_conf, master, slaves, beaver_env)
        if plugin == "conda_oap":
            # Deploy Conda OAP
            deploy_conda_oap_internal(slaves, beaver_env)

def undeploy_spark_sql_with_plugin(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_spark_sql(custom_conf, beaver_env)
    for plugin in plugins:
        if plugin == "oap":
            clean_oap(master, beaver_env)
        if plugin == "conda_oap":
            clean_conda_oap_all(slaves)

def populate_spark_sql_conf_with_plugin(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    populate_spark_sql_conf(custom_conf)
    for plugin in plugins:
        if plugin == "oap":
            # oap_compile(beaver_env, master)
            deploy_oap(custom_conf, master, slaves, beaver_env)
            # update_spark_config_with_oap(custom_conf, master, beaver_env)
            # oap_with_DCPMM_prepare(custom_conf, master, slaves, beaver_env)
        if plugin == "conda_oap":
            # Deploy Conda OAP
            deploy_conda_oap_internal(slaves, beaver_env)