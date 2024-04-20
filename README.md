# Stream Data Pipeline using Scala and AWS

This project focuses on the Initial Bulk Data Load phase for processing large datasets using Scala and AWS services. It outlines a comprehensive, step-by-step plan to establish a baseline of data for subsequent processing.

## Architecture
![Data Pipeline Architecture](architecture.jpg)

## Overview
This project implements a Big Data processing pipeline leveraging Scala and Apache Spark, integrated with various AWS services. It is structured to demonstrate batch and stream processing capabilities, as well as data analysis and visualization workflows.

## Description
The pipeline is divided into distinct stages, including initial batch data processing, stream data processing for Change Data Captures (CDC), big data analysis, and data visualization. Each component is designed to work in harmony within the AWS cloud environment.

## Technologies
- Scala: Used for writing Spark jobs due to its concise syntax and functional features.
- Apache Spark: Chosen for its ability to handle large-scale data processing.
- AWS Services:
  - Amazon RDS: Source database for ETL.
  - AWS S3: Data lake storage for raw and processed data.
  - Amazon MSK: Managed Kafka service for stream processing.
  - AWS IAM: Access and identity management for secure AWS resource handling.
  - Amazon QuickSight: BI tool for data visualization.

## Getting Started

### Prerequisites
- Scala (version specified by the project)
- Apache Spark (compatible version with Scala)
- AWS CLI configured with the necessary permissions

### Installation
Clone the repository to your local machine:
```bash
git clone git@github.com:hushuolin/ScalaStreamOnAWS.git
cd ScalaStreamOnAWS
```

# Usage

## Pipeline Operations

### Batch Data Processing
1. **Extract**: Extract data from Amazon RDS.
2. **Transform**: Transform using Spark and Scala.
3. **Load**: Load into AWS S3 for storage.

### Stream Data Processing
1. **Setup**: Set up Amazon MSK to capture data changes.
2. **Process**: Process streams with real-time jobs.

### Big Data Analysis
1. **Analyze**: Analyze S3-stored data with Spark and Scala.
2. **Store**: Store analysis results in S3.

### Data Visualization
1. **Link**: Link Amazon QuickSight to S3.
2. **Create**: Create visualizations and reports.

## Features

- Real-time data streaming with Kafka.
- Scalable and secure AWS infrastructure.
- Comprehensive data analysis and visualization.

## Contributing

Contributions are welcome! Please refer to [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines on contributions.

## Authors

- **Big Data Engineer**: [Shuolin Hu]
- **Big Data Analyst**: [Zhixin Zhang]

## License

This project is under the MIT License - see [LICENSE](./LICENSE) for more details.

## Acknowledgments

- Thanks to Professor [Robin Hillyard] and Teaching Assistant [Harshil Shahfor] their mentorship.
- Special thanks to everyone who contributed to the project.