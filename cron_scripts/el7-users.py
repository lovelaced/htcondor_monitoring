#!/bin/env python
# Polls the Condor collector

# Import some standard python utilities
import sys, time, argparse
import classad, htcondor

collectors = ["cm.chtc.wisc.edu"]

# iterate through the slots, print out the user and schedd of the claimed ones
for collector in collectors:
  coll = htcondor.Collector(collector)
  slotState = coll.query(htcondor.AdTypes.Startd, "true",['Name','RemoteGroup','JobId','State','RemoteOwner','COLLECTOR_HOST_STRING','Cpus', 'OpSysMajorVer'])
  timestamp = str(int(time.time()))
  for slot in slotState[:]:
    if (slot['State'] == "Owner") or (slot['State'] == "Unclaimed") or (slot['State'] == "Preempting") or (slot['State'] == "Matched"):  ## If slot is in owner state there is no RemoteOwner or RemoteGroup
      if (slot['OpSysMajorVer'] == 7):
        print slot['State'] + ' none ' + slot['Name'] + " " + str(slot['Cpus']) + " " + str(slot['OpSysMajorVer'])
    if (slot['State'] == "Claimed"):
      for cpu in range(0,int(slot['Cpus'])):
        if (slot['OpSysMajorVer'] == 7):
          print slot['State'] + " " + ' '.join(slot['RemoteOwner'].split("@")) + slot['Name'] + " " + str(slot['Cpus']) + " " + str(slot['OpSysMajorVer'])
