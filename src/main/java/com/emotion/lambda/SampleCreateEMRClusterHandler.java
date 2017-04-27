package com.emotion.lambda;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.text.SimpleDateFormat;
import java.util.Calendar;


public class SampleCreateEMRClusterHandler implements RequestHandler<Object, String> {

    public String handleRequest(Object event, Context context) {
        AWSCredentialsProvider cp = new EnvironmentVariableCredentialsProvider();
        AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(cp);
        emr.setRegion(Region.getRegion(Regions.US_EAST_1));

        JobFlowInstancesConfig instanceConfig = new JobFlowInstancesConfig()
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withInstanceGroups(buildInstanceGroupConfigs());

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("EmotionCluster")
                .withReleaseLabel("emr-5.0.3")
                .withLogUri("s3://emotion-s3/emotion-emr/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withVisibleToAllUsers(true)
                .withInstances(instanceConfig)
                .withApplications(buildApplications())
                .withSteps(buildStepConfigs());

        RunJobFlowResult result = emr.runJobFlow(request);
        return "Process complete.";
    }

    private List<Application> buildApplications() {
        List<Application> apps = new ArrayList<Application>();
        apps.add(new Application().withName("Hadoop"));
        apps.add(new Application().withName("Spark"));
        return apps;
    }

    private List<InstanceGroupConfig> buildInstanceGroupConfigs() {
        List<InstanceGroupConfig> result = new ArrayList<InstanceGroupConfig>();
        InstanceGroupConfig masterInstance = new InstanceGroupConfig()
                .withName("MasterNode")
                .withInstanceRole(InstanceRoleType.MASTER)
                .withInstanceCount(1)
                .withInstanceType("m3.xlarge");
        result.add(masterInstance);

        InstanceGroupConfig coreInsetance = new InstanceGroupConfig()
                .withName("CoreNode")
                .withInstanceRole(InstanceRoleType.CORE)
                .withInstanceCount(1)
                .withInstanceType("m3.xlarge");
        result.add(coreInsetance);

        return result;
    }

    private List<StepConfig> buildStepConfigs() {
        List<StepConfig> result = new ArrayList<StepConfig>();
        
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String now = sdf.format(cal.getTime());

        final String[] args = {
                "spark-submit",
                "--deploy-mode", "cluster",
                "--class", "SparkExampleApp",
                "s3://emotion-s3/emotion-emr/src/emotion-linear-regression_2.11-1.0.jar", 
                "s3n://emotion-s3/emotion-emr/transition.csv",
                "s3n://emotion-s3/emotion-emr/output/output-" + now + ".csv"
        };

        final StepConfig sparkStep = new StepConfig()
                .withName("SparkProcess")
                .withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar("command-runner.jar")
                        .withArgs(args));
        result.add(sparkStep);

        return result;
    }

}