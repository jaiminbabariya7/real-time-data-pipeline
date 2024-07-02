---
Title: "Project 1: Real Time Data PIpeline with Python"
Author: "Jaimin Babariya"
Date: "`Tue 2 Jul 2024`"
---

# Introduction

The aim of this project is to build a real-time data pipeline using Python. The pipeline includes several key components: data ingestion, transformation, and storage. By leveraging Google Cloud Platform (GCP) services such as Pub/Sub, Dataflow, and Google Cloud Storage (GCS), we can create an efficient and scalable data pipeline.

# Project Outline

## 1. Create a Repository

We start by creating a GitHub repository to manage our project code. This repository will serve as the central location for all the scripts and documentation related to the project.

## 2. Data Ingestion (Pub/Sub)

### Objective

The goal is to simulate data ingestion using Google Cloud Pub/Sub. Pub/Sub is a messaging service that allows us to publish and subscribe to messages in real-time.

### Process

- **Publisher**: Simulates the publishing of messages to a Pub/Sub topic.
- **Subscriber**: Receives and processes messages from the Pub/Sub subscription.

## 3. Data Transformation (Dataflow)

### Objective

The data received from Pub/Sub will be transformed using Apache Beam, which will run on Google Cloud Dataflow. The transformation process includes cleaning, filtering, and enriching the data to make it suitable for analysis.

### Process

- **Read from Pub/Sub**: Ingest data from the Pub/Sub subscription.
- **Transformation**: Apply various transformations to the data such as decoding, filtering, and enriching.
- **Write to GCS**: Store the transformed data in Google Cloud Storage.

## 4. Data Storage (GCS/BigQuery)

### Objective

Store the transformed data in a persistent and queryable format. Google Cloud Storage (GCS) and BigQuery are used for this purpose.

# Implementation Steps

## Step 1: Setting Up the Environment

- Create a Google Cloud Project.
- Enable necessary APIs (Pub/Sub, Dataflow, GCS).
- Set up authentication and roles.

## Step 2: Creating the Pub/Sub Topic and Subscription

- Create a Pub/Sub topic for publishing messages.
- Create a subscription to listen to messages from the topic.

## Step 3: Building the Dataflow Pipeline

- Design and implement the data transformation logic using Apache Beam.
- Deploy the pipeline to Google Cloud Dataflow.

## Step 4: Configuring Data Storage

- Set up a GCS bucket for storing transformed data.
- (Optional) Set up a BigQuery dataset and table for storing the transformed data from GCS Bucket for further analytics.

# Project Documentation

## Diagram

Refer to Real_Time_Data_Pipeline.png file for the visual representation of the Data Pipeline.

## Components Description

- **Pub/Sub**: Handles the ingestion of raw data messages.
- **Dataflow**: Transforms the data by applying various operations.
- **GCS/BigQuery**: Stores the transformed data for further analysis.

# Conclusion

This project demonstrates how to build a real-time data pipeline using Google Cloud services. By following the outlined steps, we can effectively ingest, transform, and store data in a scalable manner. The integration of Pub/Sub, Dataflow, and GCS/BigQuery ensures that the pipeline is both efficient and flexible, catering to a variety of data processing needs.

