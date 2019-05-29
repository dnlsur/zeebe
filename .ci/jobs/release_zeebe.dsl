// vim: set filetype=groovy:

pipelineJob('zeebe-release') {
    displayName 'Zeebe Release'
    definition {
        cps {
            script(readFileFromWorkspace('.ci/pipelines/release_zeebe.groovy'))
            sandbox()
        }
    }

    parameters {
        string('RELEASE_VERSION', '0.x.0', 'Which version to release?')
        string('DEVELOPMENT_VERSION', '0.y.0-SNAPSHOT', 'Next development version?')
        booleanParam('PUSH_CHANGES', true, 'Push release to remote repositories and deploy docs?')
        booleanParam('IS_LATEST', true, 'Should the docker image be tagged as camunda/zeebe:latest?')
    }
}
