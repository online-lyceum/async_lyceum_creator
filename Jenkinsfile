#!groovy

pipeline {
    agent any
    stages {
        stage("Build") {
            steps {
                sh 'docker build -t lesson-creator .'
            }
        }
        stage("Run images") {
            steps {
                sh 'docker-compose up -d'
            }
        }
    }
}
