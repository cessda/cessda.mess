pipeline {
    agent any

    environment {
        DOCKER_IMAGE = "cessda/mess"
        DOCKER_TAG   = "${env.BUILD_NUMBER}"
    }

    stages {

        stage('Lint') {
            steps {
                sh 'uv run ruff check app tests'
            }
        }

        stage('Test') {
            environment {
                POSTGRES_PASSWORD = credentials('mess-postgres-password')
            }
            steps {
                sh 'uv run pytest --tb=short'
            }
            post {
                always {
                    junit 'reports/junit.xml'
                    publishHTML(target: [
                        reportDir: 'htmlcov',
                        reportFiles: 'index.html',
                        reportName: 'Coverage Report'
                    ])
                }
            }
        }

        stage('Build Docker image') {
            when { branch 'main' }
            steps {
                sh "docker build -t ${DOCKER_IMAGE}:${DOCKER_TAG} -t ${DOCKER_IMAGE}:latest ."
            }
        }

        stage('Push Docker image') {
            when { branch 'main' }
            steps {
                withDockerRegistry([credentialsId: 'docker-hub-credentials']) {
                    sh "docker push ${DOCKER_IMAGE}:${DOCKER_TAG}"
                    sh "docker push ${DOCKER_IMAGE}:latest"
                }
            }
        }
    }

    post {
        failure {
            echo 'Build failed. Check the logs for details.'
        }
        success {
            echo 'Build succeeded.'
        }
    }
}
