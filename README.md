# Introduction

# Features

# Usage (Local)
To run the 

# Usage (AWS)

# Usage (Grafana)
Grafana is used to measure real-time performance, which uses 

# Summary of Report
Full report can be found at u7056839_NFI_COMP631-Ass3.docx. The document contains
- Wireshark analysis
- Codebase design decision
## Key Statistics

# Draft Report
## Methodology 
### Testing Condition 1: All Local
### Testing Condition 2: Local Analyser + Remote (Publisher + Broker)
### Testing Condition 3: Local Analyser + Remote Publisher + Remote Broker

## Discussion
### Question 1
```
Wireshark the handshake for one example of each of the differing QoS-levels (0,1,2), include screenshots in your report that show the wireshark capture of your subcription (filter for mqtt), and briefly explain how each QoS-level transfer works, and what it implies for message duplication and message order.

Discuss briefly in your report in which circumstances would you choose each QoS level, on pub->broker and broker->sub. [around 0.5 page of text]
```
### Question 2
```
Summarise your measurements from above, in suitable table form, and with simple charts, to compare the impact of different message sizes, delays, and QoS combinations.

Explain what you expected to see, especially in relation to the different QoS levels, and whether your expectations were matched. 

Also describe what correlations of measured rates with $SYS topics you expected to see and why, and whether you do, or do not
```

### Question 3
```
Consider the broader end-to-end (internet-wide) network environment, in a situation with millions of sensors publishing frequently to thousands of subscribers. Explain in your report [around 1 page]

a. What performance challenges might be when using MQTT for extremely high volumes of messages, from the sources publishing their messages, all the way through the network and broker to your subscribing client application. If you lose messages, where might they be lost, and why? Think about links, routers, memory/buffers, cpus, long paths/high delays, layer 3/4/7, etc.
b. How the different QoS levels may help, or not, in dealing with the challenges.
c. Why ‘retaining’ messages would be a bad idea in this context.
```
# Development Log
## Sat 23/05/26
Started real development at this stage. Completed

## Mon 25/05/26
Setup Grafana+InfluxDB stack for real time performance metric. Likely add