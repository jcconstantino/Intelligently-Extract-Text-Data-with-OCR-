# Intelligently Extract Text & Data with OCR  
## Amazon Textract + S3 + Lambda + DynamoDB

## 📌 Overview
This project demonstrates how to build a **serverless OCR (Optical Character Recognition) pipeline** using AWS services.  
It automatically extracts text and structured data from uploaded documents and stores the results for further processing.

## 🧠 Use Case
- Document digitization
- Invoice and receipt processing
- Form data extraction
- Automating manual encoding tasks

---

## 🏗️ Architecture

1. Upload document to **Amazon S3**
2. Trigger **AWS Lambda** function
3. Lambda calls **Amazon Textract** for OCR processing
4. Extracted data is parsed and stored in **Amazon DynamoDB**
5. Results can be used for analytics, APIs, or applications

---

## ⚙️ AWS Services Used

- **Amazon S3** – Stores uploaded documents
- **AWS Lambda** – Processes events and orchestrates workflow
- **Amazon Textract** – Extracts text and structured data
- **Amazon DynamoDB** – Stores extracted results

---

## 🔄 Workflow

```text
User Upload → S3 Bucket → Lambda Trigger → Textract OCR → DynamoDB Storage


https://github.com/jcconstantino/Intelligently-Extract-Text-Data-with-OCR-/blob/main/image.png
