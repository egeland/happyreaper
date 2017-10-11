#!/usr/bin/env python3

# happyreaper - helps you clean up stuck AWS EBS volumes in statefulsets.
# Copyright (C) 2017  Frode Egeland <egeland@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from delorean import now, Delorean, parse
from kubernetes import client, config
import urllib.request
import boto3
import os

global ANNOTATION_KEY
ANNOTATION_KEY = "happyreaper/last-touch"
global current_time
current_time = now()
global MAX_AGE
MAX_AGE = int(os.getenv("MAX_AGE", 60)) * 60  # minutes (convert to sec)
global MAX_RESTART
MAX_RESTART = int(os.getenv("MAX_RESTART", 20))
global DRYRUN
DRYRUN = os.getenv("DRYRUN", False) == "True" or os.getenv("DRYRUN", False) == "true"
global AWS_REGION
AWS_REGION = urllib.request.urlopen("http://169.254.169.254/latest/meta-data/placement/availability-zone").read()[:-1]


def container_info(pod):
    restart_count = 0
    container_creating = False
    for c in pod.status.container_statuses:
        restart_count += c.restart_count
        if c.state.waiting and c.state.waiting.reason == "ContainerCreating":
            container_creating = True
    return (restart_count, container_creating)


def is_statefulset(pod):
    for ref in pod.metadata.owner_references:
        if ref.kind == "StatefulSet":
            return True
    return False


def is_ok_to_touch(pod):
    if pod.metadata.annotations:
        if ANNOTATION_KEY in pod.metadata.annotations:
            return int(current_time - parse(pod.metadata.annotations[ANNOTATION_KEY])) >= MAX_AGE
    return True


def annotate_pod(pod):
    if DRYRUN:
        print("DRYRUN: skipping annotate_pod step")
    else:
        pod.metadata.annotations[ANNOTATION_KEY] = current_time.datetime
        pod = v1.patch_namespaced_pod(name=pod.metadata.name, namespace=pod.metadata.namespace, body=pod)


def detach_volume(pod):
    pvc = v1.read_namespaced_persistent_volume_claim(name=find_pvc(pod), namespace=pod.metadata.namespace)
    volume_name = pvc.spec.volume_name
    pv = v1.read_persistent_volume(volume_name)
    volume_id = pv.spec.aws_elastic_block_store.volume_id
    global ec2
    if not ec2:
        ec2 = boto3.client(service_name="ec2", region_name=AWS_REGION)
    if DRYRUN:
        print("DRYRUN: skipping detach_volume step")
    else:
        ec2.detach_volume(VolumeId=volume_id, Force=True)
    annotate_pod(pod)


def find_pvc(pod):
    for vol in pod.spec.volumes:
        if vol.persistent_volume_claim and vol.persistent_volume_claim.claim_name:
            return vol.persistent_volume_claim.claim_name
    return None


def evict_pod(pod):
    delete_options = client.V1DeleteOptions(grace_period_seconds=30)
    eviction = client.V1beta1Eviction(delete_options=delete_options, metadata=pod.metadata)
    if DRYRUN:
        print("DRYRUN: skipping evict_pod step")
    else:
        v1.create_namespaced_pod_eviction(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            body=eviction
        )
    annotate_pod(pod)


def main():
    config.load_incluster_config()

    global v1
    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(watch=False)
    for pod in ret.items:
        if pod.status.phase in ["Running", "Succeeded"] or not is_ok_to_touch(pod):
            continue
        restart_count, is_container_creating = container_info(pod)
        if is_container_creating:
            # figure out age
            elapsed_time = current_time - Delorean(pod.status.start_time)
            elapsed_time = int(elapsed_time.total_seconds())
            if elapsed_time > MAX_AGE:
                # if it's been stuck for more than MAX_AGE
                # and it is a statefulset, we should detach its volume,
                # otherwise evict the pod
                if is_statefulset(pod):
                    detach_volume(pod)
                else:
                    evict_pod(pod)
                continue
        if restart_count >= MAX_RESTART:
            evict_pod(pod)


if __name__ == '__main__':
    main()
