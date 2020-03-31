# Lambda Architecture for Twitter Real-time sentiment analysis
Lambda Architecture implementation using Apache Storm, Hadoop and HBase to perform Twitter real-time sentinent analysis

## Table of Contents

* [About the Project](#about-the-project)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
* [Usage](#usage)


## About The Project
![Diagram of the Lambda Architecture structure](https://github.com/LorenzoAgnolucci/TwitterRealTimeSentimentAnalysis/blob/master/latex/paper_lambda_architecture_PC/img/png_lambda_architecture_diagram.png)

Lambda Architecture implementation using Apache Storm, Hadoop and HBase to perform Twitter real-time sentinent analysis.

For more information, read the [paper](paper_lambda_architecture_PC.pdf) located in the repo root.

### Built With

* [Apache Hadoop 3.2.1](https://hadoop.apache.org)
* [Apache HBase 2.2.3](https://hbase.apache.org)
* [Apache Storm 2.1.0](https://storm.apache.org)
* [LingPipe 4.1.0](http://alias-i.com/lingpipe)
* [Twitter4J](http://twitter4j.org/en/)



## Getting Started

To get a local copy up and running follow these simple steps.

### Prerequisites
Apache Storm, Hadoop and HBase installed and correctly configured in your cluster.

## Usage

First you have to get your personal Twitter Developers credentials and write it in a .txt file with the same structure as '''DummyTwitterCredentials'''.

Then start Apache Storm, Hadoop and HBase in your cluster.

Finally execute (in this order):

* '''TwitterSentimentAnalysisTopology.java'''
* '''PresentationLayer.java'''
* '''HadoopDriver.java'''

## Authors

* **Lorenzo Agnolucci**


## Acknowledgments
Parallel Computing Project © Course held by Professor [Marco Bertini](https://www.unifi.it/p-doc2-2019-0-A-2b333d2d3529-1.html) - Computer Engineering Master Degree @[University of Florence](https://www.unifi.it/changelang-eng.html)

