import os
from copy import copy
import datetime
import time
import random
from enum import Enum

from kubernetes import client, config, watch
import slack # https://slack.dev/python-slack-sdk/web/index.html
from slack_sdk.errors import SlackApiError

def _generate_progress_bar(position, max_value):
    if position is None: position = 0
    if max_value is None or max_value == 0: max_value = 1

    filled_squares = (100 / max_value * position) / 5

    filled_char = "⬛"
    empty_char = "⬜"
    return (filled_char * int(filled_squares)) + (
            empty_char * (20 - int(filled_squares))) + "\n"

# There are really only three states that things can be in
class KubeStatus(Enum):
    TIMED_OUT = 1
    PROGRESSING = 2
    COMPLETE = 3

# We maintain separate counts (and slack threads) for intentional updates (deployments)
# vs. things falling over (degraded).
# Note that we are using "deployment" as a verb to describe a thing we are doing,
# not as in the kubernetes object type.
class KubeEvent(Enum):
    DEPLOYMENT = 1
    DEGRADED = 2

class KubeLookout:
    template = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ""
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ""
            },
            "accessory": {
                "type": "image",
                "image_url": "",
                "alt_text": "status image"
            }
        }
    ]

    def __init__(self, warning_image, progress_image, recovering_image, ok_image,
                 slack_key, slack_deploy_channel, slack_alert_channel,
                 cluster_name, gcp_region, gcp_project,
                 thread_refresh, thread_timeout):
        super().__init__()
        self.warning_image = warning_image
        self.ok_image = ok_image
        self.progress_image = progress_image
        self.recovering_image = recovering_image
        self.slack_client = None
        self.slack_key = slack_key
        self.slack_deploy_channel = slack_deploy_channel
        self.slack_alert_channel = slack_alert_channel
        self.cluster_name = cluster_name
        self.gcp_region = gcp_region
        self.gcp_project = gcp_project
        self.thread_refresh = thread_refresh
        self.thread_timeout = thread_timeout
        self.thread_head = { KubeEvent.DEPLOYMENT: None, KubeEvent.DEGRADED: None }
        self.deployment_count = 0 # total number of deploys currently being tracked, including completed ones
        self.degraded_count = 0 # total number of degraded apps being tracked, including recovered ones
        self.deployments = {} # active deploys being tracked
        self.degraded = {} # active degraded apps being tracked
        self.problems = {}

    def _init_client(self):
        if "KUBERNETES_PORT" in os.environ:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        api_client = client.api_client.ApiClient()
        self.core = client.AppsV1Api(api_client)

    def _send_slack_block(self, blocks, channel, message_id=None, thread_ts=None):
        if self.slack_client is None:
            self.slack_client = slack.WebClient(
                self.slack_key)

        if thread_ts and \
            (datetime.datetime.now().timestamp() - self.thread_refresh) > float(thread_ts):
            # The thread is too old!  Refresh the channel with it so it doesn't get lost
            reply_broadcast = True
        else:
            reply_broadcast = False

        try:
            if message_id is None:
                response = self.slack_client.chat_postMessage(channel=channel,
                                                            blocks=blocks,
                                                            icon_emoji=':kubernetes:',
                                                            thread_ts=thread_ts,
                                                            reply_broadcast=reply_broadcast,
                                                            unfurl_links='false')
                return response.data['ts'], response.data['channel']
            response = self.slack_client.chat_update(
                channel=channel,
                thread_ts=thread_ts,
                ts=message_id, blocks=blocks)
            return response.data['ts'], response.data['channel']
        except SlackApiError as e:
            print(f"Could not send message to slack API: {e}")
            if e['error'] == 'ratelimited':
                time.sleep(random.randrange(1,30))

    def _handle_deployment_change(self, deployment):
        metadata = deployment.metadata
        # we are not going to concern ourselves with changes to Kubernetes system stuff
        if (metadata.namespace == 'kube-system'):
            return
        deployment_key = f"{metadata.namespace}/{metadata.name}"
        print(f"{datetime.datetime.now()} Handling deployment of {deployment_key}")

        ready_replicas = 0
        if deployment.status.ready_replicas is not None:
            ready_replicas = deployment.status.ready_replicas

        if deployment_key not in self.deployments and \
                deployment.status.updated_replicas is None:
            blocks = self._generate_deployment_rollout_block(deployment)
            print(f"139 blocks {blocks[0]}")
            resp = self._send_slack_block(blocks, self.slack_deploy_channel, thread_ts=self._thread_head_ts(type=KubeEvent.DEPLOYMENT))
            self.deployments[deployment_key] = resp
            self.deployment_count += 1
            print(f"{datetime.datetime.now()} rollout added: {deployment_key}")
            self._update_thread_head(type=KubeEvent.DEPLOYMENT)

        elif deployment_key in self.deployments:
            rollout_complete = (
                    deployment.status.updated_replicas ==
                    deployment.status.replicas ==
                    ready_replicas)
            blocks = self._generate_deployment_rollout_block(deployment,
                                                             rollout_complete)
            print(f"153 blocks {blocks[0]}")
            self.deployments[deployment_key] = self._send_slack_block(
                channel=self.deployments[deployment_key][1],
                message_id=self.deployments[deployment_key][0], blocks=blocks,
                thread_ts=self._thread_head_ts(type=KubeEvent.DEPLOYMENT))

            if rollout_complete:
                self.deployments.pop(deployment_key)
                print(f"rollout complete for {deployment_key}")
            elif blocks[1]['accessory']['image_url'] == self.warning_image:
                self.deployments.pop(deployment_key)
                print(f"rollout failed for {deployment_key}")
                self.problems[deployment_key] = True
            else:
                print(f"{datetime.datetime.now()} rollout updated for {deployment_key}")

            self._update_thread_head(type=KubeEvent.DEPLOYMENT)

        elif ready_replicas < deployment.spec.replicas:
            print(f"Detected degraded {deployment_key}" +
                  f" {ready_replicas} ready out of {deployment.spec.replicas}")
            blocks = self._generate_deployment_degraded_block(deployment)
            if deployment_key in self.degraded and self.degraded[deployment_key][1]:
                degraded_slack_channel=self.degraded[deployment_key][1]
                message_id=self.degraded[deployment_key][0]
            else:
                degraded_slack_channel=self.slack_alert_channel
                message_id=None
            thread_head = self._thread_head_ts(type=KubeEvent.DEGRADED)
            print(f"182 blocks {blocks[0]}")
            self.degraded[deployment_key] = self._send_slack_block(
                blocks, degraded_slack_channel, message_id=message_id,
                thread_ts=thread_head)
            self.degraded_count += 1
            self._update_thread_head(type=KubeEvent.DEGRADED)

        elif (deployment_key in self.degraded and \
              ready_replicas >= deployment.spec.replicas):
            print(f"{datetime.datetime.now()} Recovered degraded {deployment_key}" +
                  f" {ready_replicas} ready out of {deployment.spec.replicas}")
            blocks = self._generate_deployment_not_degraded_block(deployment)
            print(f"193 blocks {blocks[0]}")
            thread_head = self._thread_head_ts(type=KubeEvent.DEGRADED)
            self._send_slack_block(blocks, self.degraded[deployment_key][1],
                                   message_id=self.degraded[deployment_key][0],
                                   thread_ts=thread_head)
            self.degraded.pop(deployment_key)
            self._update_thread_head(type=KubeEvent.DEGRADED)

    def _thread_head_ts(self, type):
        # If we have an appropriate thread head for this type of event (either deploy or degraded)
        # then return the thread_ts (thread timestamp, which acts as message ID).
        # If the existing thread head is too old, start a new one.
        # If there is no existing thread head, then start one.
        debug_activity = f"(rollouts: {self.deployments})" if (type == KubeEvent.DEPLOYMENT) else f"(degraded: {self.degraded})"

        if self.thread_head[type] and (datetime.datetime.now().timestamp() - self.thread_timeout) > float(self.thread_head[type][0]):
            # Our thread is SO OLD.  Give up on it and start fresh
            print(f"{datetime.datetime.now()} Timing out thread {self.self.thread_head[type][0]} {debug_activity}")
            blocks = self._generate_thread_head_block(type=type, status=KubeStatus.TIMED_OUT)
            print(f"211 blocks {blocks[0]}")
            resp = self._send_slack_block(blocks=blocks, channel=self.self.thread_head[type][1], message_id=self.thread_head[type][0])
            self.thread_head[type] = None
            self.problems = {}

        if self.thread_head[type] is None:
            blocks = self._generate_thread_head_block(type=type, status=KubeStatus.PROGRESSING)
            print(f"218 blocks {blocks[0]}")
            resp = self._send_slack_block(blocks, self.slack_deploy_channel)
            print(f"Started new thread {resp[0]} {debug_activity}")
            self.thread_head[type] = resp
            self.problems = {}

        return self.thread_head[type][0]

    def _update_thread_head(self, type):
        if (type == KubeEvent.DEPLOYMENT):
            debug_activity = f"(rollouts: {len(self.deployments)}/{self.deployment_count})"
        else:
            debug_activity = f"(degraded: {len(self.degraded)}/{self.degraded_count})"
        print(f"{datetime.datetime.now()} Updating thread head {self.thread_head[type][0]} {debug_activity}")
        # if (self.deployment_count == 0 and type == KubeEvent.DEPLOYMENT) or \
        #     (self.degraded_count == 0 and type == KubeEvent.DEGRADED):
        #     # Nothing has started yet, too soon to update!
        #     return
        
        try:
            if (type == KubeEvent.DEPLOYMENT and len(self.deployments) == 0) or \
                (type == KubeEvent.DEGRADED and len(self.degraded) == 0):
                print(f"{datetime.datetime.now()} Marking thread complete")
                blocks = self._generate_thread_head_block(type=type, status=KubeStatus.COMPLETE)
                print(f"242 blocks {blocks[0]}")
                resp = self._send_slack_block(blocks=blocks, channel=self.thread_head[type][1], message_id=self.thread_head[type][0])
                self.thread_head[type] = None
                if type == KubeEvent.DEPLOYMENT: self.deployment_count = 0
                else: self.degraded_count = 0
            else:
                print(f"{datetime.datetime.now()} Marking thread in progress")
                blocks = self._generate_thread_head_block(type=type, status=KubeStatus.PROGRESSING)
                print(f"250 blocks {blocks[0]} {self.thread_head[type]}")
                resp = self._send_slack_block(blocks=blocks, channel=self.thread_head[type][1], message_id=self.thread_head[type][0])
        except Exception as e:
            print(f"Failed to update slack block: {e}")

    def _handle_event(self, deployment):
        if deployment.metadata.namespace != 'kube-system':
            if ((len(self.deployments) + len(self.degraded)) % 10) == 9:
                print("Pausing a moment to reduce the risk of htting slack rate limits")
                time.sleep(random.randrange(1,5))
            self._handle_deployment_change(deployment)

    def main_loop(self):
        while True:
            self._init_client()
            pods = self.core.list_deployment_for_all_namespaces(watch=False)
            resource_version = pods.metadata.resource_version
            stream = watch.Watch().stream(
                self.core.list_deployment_for_all_namespaces,
                resource_version=resource_version
            )
            print("Watching for deployment events")
            for event in stream:
                # print("Event: %s %s" % (event['type'], event['object'].metadata.name))
                deployment = event['object']
                self._handle_event(deployment)

    def _generate_deployment_rollout_block(self, deployment,
                                           rollout_complete=False):

        block = copy(self.template)
        header = f"*{self.gcp_project} deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" is rolling out an update.*"
        message = ''
        for container in deployment.spec.template.spec.containers:
            message += f"Container {container.name} has image " \
                f"_ {container.image} _" \
                f"_ https://console.cloud.google.com/kubernetes/deployment/{self.gcp_region}/{self.cluster_name}/{deployment.metadata.namespace}/{deployment.metadata.name}/overview?project={self.gcp_project} _\n"
        message += "\n"

        # this math assumes a certain deployment logic -- spin up some new replicas before
        # spinning down the old -- but it's the logic we usually use
        unavailable = 0 if str(deployment.status.unavailable_replicas) == 'None' else deployment.status.unavailable_replicas
        updated = 0 if str(deployment.status.updated_replicas) == 'None' else deployment.status.updated_replicas
        print(f"{datetime.datetime.now()}" + " " + \
              deployment.metadata.namespace + "/" + deployment.metadata.name + \
              " unavailable: " + str(unavailable) + \
              " updated: " + str(updated))
        for condition in deployment.status.conditions:
            print("%s %s/%s %s=%s (%s)" % (condition.last_update_time,
                                           deployment.metadata.namespace,
                                           deployment.metadata.name,
                                           condition.type,
                                           condition.status,
                                           condition.message))
            
        live_updates = 0 if (updated < unavailable) else updated - unavailable
    
        message += f"{live_updates} replicas " \
            f"updated out of " \
            f"{deployment.spec.replicas}, {deployment.status.ready_replicas}" \
            f" ready.\n\n"
        if self.problems:
            message += f"{len(self.problems)} deployments are in trouble"
        message += _generate_progress_bar(live_updates, deployment.spec.replicas)

        block[0]['text']['text'] = header
        block[1]['text']['text'] = message
        block[1]['accessory']['image_url'] = self.progress_image
        self.problems.pop(f"{deployment.metadata.namespace}/{deployment.metadata.name}", None)
        # if the deployment status reflects that it is no longer progressing,
        # update the image to reflect that
        if deployment.status.conditions[-1].type == "Progressing" and \
            deployment.status.conditions[-1].status == "False":
            block[1]['accessory']['image_url'] = self.warning_image
            block[0]['text']['text'] = f"*{self.gcp_project} deployment " \
                f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
                f" is failing: {deployment.status.conditions[-1].message}*"
            print(f"{deployment.metadata.namespace}/{deployment.metadata.name} is troubled: {deployment.status.conditions} (problems: {self.problems})")
            self.problems[f"{deployment.metadata.namespace}/{deployment.metadata.name}"] = True
        # when rollout is complete, update our image
        if rollout_complete:
            block[1]['accessory']['image_url'] = self.ok_image
            self.problems.pop(f"{deployment.metadata.namespace}/{deployment.metadata.name}", None)
        return block

    def _generate_deployment_degraded_block(self, deployment):

        block = copy(self.template)

        header = f"*{self.gcp_project} deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" has become degraded.*"

        ready_replicas = 0 if deployment.status.ready_replicas is None else deployment.status.ready_replicas
        message = f"Deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" has {ready_replicas} ready replicas " \
            f"when it should have {deployment.spec.replicas}.\n"

        message += _generate_progress_bar(deployment.status.ready_replicas,
                                          deployment.spec.replicas)

        block[0]['text']['text'] = header
        block[1]['text']['text'] = message
        block[1]['accessory']['image_url'] = self.warning_image

        return block

    def _generate_deployment_not_degraded_block(self, deployment):
        block = copy(self.template)

        header = f"*{self.gcp_project} deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" is no longer in a degraded state.*"

        message = f"Deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" has {deployment.status.ready_replicas} ready " \
            f"replicas out of " \
            f"{deployment.spec.replicas}.\n"

        message += _generate_progress_bar(deployment.status.ready_replicas,
                                          deployment.spec.replicas)

        block[0]['text']['text'] = header
        block[1]['text']['text'] = message
        block[1]['accessory']['image_url'] = self.ok_image

        return block

    def _generate_thread_head_block(self, type, status):

        block = copy(self.template)

        if type == KubeEvent.DEPLOYMENT and self.deployment_count == 0: bar_max = 1
        if type == KubeEvent.DEPLOYMENT: bar_max = self.deployment_count
        elif type == KubeEvent.DEGRADED and self.degraded_count == 0: bar_max = 1
        else: bar_max = self.degraded_count

        if self.problems:
            status_message = "having problems"
            status_image = self.recovering_image
        elif status == KubeStatus.TIMED_OUT:
            status_message = "timed out"
            status_image = self.warning_image
        elif type == KubeEvent.DEPLOYMENT and status == KubeStatus.PROGRESSING:
            status_message = "being updated"
            status_image = self.progress_image
        elif type == KubeEvent.DEPLOYMENT and status == KubeStatus.COMPLETE:
            status_message = "up to date"
            status_image = self.ok_image
        elif type == KubeEvent.DEGRADED and status == KubeStatus.PROGRESSING:
            status_message = "failing healthcheck and restarting"
            status_image = self.recovering_image
        elif type == KubeEvent.DEGRADED and status == KubeStatus.COMPLETE:
            status_message = "recovered"
            status_image = self.ok_image
        else:
            status_message = "unknown"
            status_image = self.warning_image

        print(f"{datetime.datetime.now()} Generating new thread head block with {status_message} (type: {type}, status: {status})")

        if bar_max == 1:
            header = f"*A kubernetes workload in {self.gcp_project} is {status_message}*"
        else:
            header = f"*Kubernetes workloads in {self.gcp_project} are {status_message}*"
        message = f"See the slack thread under this message for details\n"
        if type == KubeEvent.DEPLOYMENT:
            message += f"Progress: {len(self.deployments)} remaining out of {self.deployment_count}\n"
            message += _generate_progress_bar(bar_max - len(self.deployments), bar_max)
        else:
            message += f"Progress: {len(self.degraded)} remaining out of {self.degraded_count}\n"
            message += _generate_progress_bar(bar_max - len(self.degraded), bar_max)

        block[0]['text']['text'] = header
        block[1]['text']['text'] = message
        block[1]['accessory']['image_url'] = status_image

        return block

if __name__ == "__main__":
    env_warning_image = os.environ.get("WARNING_IMAGE",
                                       "https://www.rocketlawyer.com/images/ops/warning.png")
    # https://www.rocketlawyer.com/images/ops/progress.gif
    env_progress_image = os.environ.get("PROGRESS_IMAGE",
                                        "https://64.media.tumblr.com/345127a42a4baf76158920730f808f3b/tumblr_nak5muSmwi1r2geqjo1_500.gifv")
    env_recovering_image = os.environ.get("RECOVERING_IMAGE",
                                          "https://64.media.tumblr.com/a1acb16e4b116ae6950d93c086914978/tumblr_n6uulrbQTO1r2geqjo1_500.gifv")
    env_ok_image = os.environ.get("OK_IMAGE",
                                  "https://www.rocketlawyer.com/images/ops/ok.png")
    env_slack_token = os.environ["SLACK_TOKEN"]
    env_slack_deploy_channel = os.environ.get("SLACK_CHANNEL", "#robot_dreams")
    env_slack_alert_channel = os.environ.get("SLACK_ALERT_CHANNEL", env_slack_deploy_channel)
    env_cluster_name = os.environ.get("CLUSTER_NAME", "kubernetes")
    env_gcp_region = os.environ.get("GCP_REGION", "us-west1")
    env_gcp_project = os.environ.get("GCP_PROJECT", "rl-us")
    env_thread_refresh = int(os.environ.get("THREAD_REFRESH", 900))
    env_thread_timeout = int(os.environ.get("THREAD_TIMEOUT", 3600))
    kube_deploy_watch = KubeLookout(env_warning_image, env_progress_image, env_recovering_image,
                                    env_ok_image, env_slack_token,
                                    env_slack_deploy_channel, env_slack_alert_channel,
                                    env_cluster_name,
                                    env_gcp_region, env_gcp_project,
                                    env_thread_refresh, env_thread_timeout)

    kube_deploy_watch.main_loop()
