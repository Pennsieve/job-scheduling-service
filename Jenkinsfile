#!groovy

node("executor") {
    checkout scm

    def commitHash = sh(returnStdout: true, script: 'git rev-parse HEAD | cut -c-7').trim()
    def imageTag = "${env.BUILD_NUMBER}-${commitHash}"

    def sbt = "sbt -Dsbt.log.noformat=true -Dversion=$imageTag"
    // sbt resolves and publishes Pennsieve libraries through AWS CodeArtifact with a
    // short-lived token from the agent's instance role (see build.sbt).

    stage("Build") {
        sh "$sbt clean +compile"
    }

    stage("Test") {
        try {
            sh "$sbt coverageOn +test"
        } finally {
            junit '**/target/test-reports/*.xml'
        }
    }

    stage("Test Coverage") {
        sh "$sbt coverageReport"
    }

    if (["main"].contains(env.BRANCH_NAME)) {
        stage("Publish Jars") {
            sh "$sbt +commons/publish"
            sh "$sbt +client/publish"
        }

        stage("Docker") {
            sh "$sbt clean docker"

            sh "docker tag pennsieve/job-scheduling-service:latest pennsieve/job-scheduling-service:$imageTag"
            sh "docker push pennsieve/job-scheduling-service:latest"
            sh "docker push pennsieve/job-scheduling-service:$imageTag"
        }

        stage("Deploy") {
            build job: "service-deploy/pennsieve-non-prod/us-east-1/dev-vpc-use1/dev/job-scheduling-service",
            parameters: [
                string(name: 'IMAGE_TAG', value: imageTag),
                string(name: 'TERRAFORM_ACTION', value: 'apply')
            ]
        }
    }
}
