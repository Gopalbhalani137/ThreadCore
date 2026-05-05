# Multi-Threaded Food Delivery Simulator

## Overview
The **Food Delivery Simulator** is a multi-stage, multi-threaded Python application that models the lifecycle of restaurant orders from preparation to delivery. Utilizing the Producer-Consumer design pattern, it accurately simulates a complex, real-world pipeline where orders are ingested, prepared by specific restaurants, and handed off to different types of delivery agents (Bikes or Cars) based on the delivery distance.

## Features
* **Multi-Stage Queuing:** Implements a sophisticated queue system where data flows from an initial ingestion queue to restaurant-specific prep queues, and finally to vehicle-specific delivery queues.
* **JSON Configuration:** Easily manage simulation variables (number of agents, available restaurants, thresholds) via a `config.json` file without altering the source code.
* **Dynamic Routing:** Automatically routes prepared orders to Bike delivery agents for short distances or Car delivery agents for long distances based on a configured threshold.
* **Concurrency & Thread Safety:** Uses `threading.Lock` and `queue.Queue` to safely manage concurrent operations across dozens of worker threads.
* **Comprehensive Analytics:** Automatically generates detailed CSV logs and a text-based summary report calculating prep times, delivery times, and overall restaurant performance.

## System Architecture
The application runs on a 3-tier Producer/Consumer pipeline:
1. **Order Producer:** Reads orders from a CSV file and pushes them into the respective restaurant's preparation queue.
2. **Restaurant Workers:** Consume orders from their specific queues, simulate preparation time, and push the prepared food into either the Bike or Car delivery queue.
3. **Consumer Agents (Bikes & Cars):** Consume from the delivery queues, simulate travel time based on distance, and log the completed jobs.

## Prerequisites
* Python 3.7+ (Requires `dataclasses`, `json`, `csv`, `threading`, `queue`)

## Configuration
Before running the project, you must set up your input files in the same directory as the script.

### 1. `config.json`
Create a JSON configuration file. The script will prompt you for this file's name when run.
```json
{
  "order_csv_file_name": "orders.csv",
  "available_restaurant_today": ["Burger King", "Pizza Hut", "Taco Bell"],
  "bike_agents": 3,
  "car_agents": 2,
  "distance_threshold": 5.0
}
```
3. When prompted, enter the name of your configuration file (e.g., `config.json`).
4. Watch the console as the multi-threaded simulation logs the preparation and delivery of each order in real-time.

## Outputs
Once the simulation is complete, the `OrderLogger` will generate two files:
* **`output.csv`**: A detailed row-by-row log of every order, including exact start and end timestamps for both the preparation and delivery phases, as well as the agent assigned.
* **`summary.txt`**: A high-level report showing the total number of orders processed per restaurant, total preparation times, total delivery times, and any skipped/failed orders.

## Future Enhancements
* **Live Order Tracking GUI:** Integrate a library like `Tkinter` or `PyQt` to build a visual dashboard where users can see orders moving from "Prep" to "Out for Delivery" to "Delivered" in real-time.
* **Agent Fatigue/Return Simulation:** Add logic so delivery agents must "drive back" to the restaurant after a delivery before they can pick up the next order in the queue.
* **Error/Cancelation Simulation:** Inject random probability for an order to be canceled during prep, or for a delivery agent to experience a delay (e.g., flat tire), testing the system's fault tolerance.
* **Database Logging:** Replace the CSV logging mechanism with a relational database (like SQLite or PostgreSQL) to allow for complex querying of historical delivery data.
