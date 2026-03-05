import csv
import threading
import time
import os
from dataclasses import dataclass
import queue
import json
from datetime import datetime

@dataclass
class OrderJob:
    '''
    dataclass represent each order 
    '''
    orderId:int
    restaurant_name:str
    distance_km:float
    prep_time_seconds:int
    delivery_agent:str
    metadata:dict

class Orderlogger:
    '''
    class to log each order details and generate report

    It handles:
      - Logging
      - CSV writing
      - Summary report generation
    These should ideally be separate components.
    '''
    def __init__(self):
        try:
            self._lock=threading.Lock()
            self._order_logs=[]
        except Exception as e:
            print(f"[Logger] Error During initialization ,{str(e)}")

    '''To add each job details to log'''
    def log_job(self,job):
        try:
            log_entry={
                'order_id':job.orderId,
                'restaurant_name':job.restaurant_name,
                'distance_km':job.distance_km,
                'prep_time_seconds':job.prep_time_seconds,
                'prep_start_time':job.metadata.get('prep_start_time'),
                'prep_end_time':job.metadata.get('prep_end_time'),
                'delivery_start_time':job.metadata.get('delivery_start_time'),
                'delivery_end_time':job.metadata.get('delivery_end_time'),
                'delivery_by':job.delivery_agent

            }
            with self._lock:
                self._order_logs.append(log_entry)
        except Exception as e:
            print(f"[Logger] Error logging job {job.orderId} : {str(e)}")

    '''To generate final reports'''
    def generate_reports(self,jobs):
        try:
            for job in jobs:
                self.log_job(job)
            self._write_csv_report('output.csv', self._order_logs)
            self._write_summary_report('summary.txt',jobs)
        except Exception as e:
            print(f"[Logger] ERROR generating reports: {str(e)}")

    '''To write summary report'''
    def _write_summary_report(self,file_name,jobs):
        try:
            resturant_summary={}
            total_preparation_time=0
            total_delivery_time=0
            not_prepared_orders=0
            for job in jobs:
                if job.metadata["order_delivered"]==1:
                    if job.restaurant_name in resturant_summary:
                        resturant_summary[job.restaurant_name]["Orders Prepared"]+=1
                        resturant_summary[job.restaurant_name]["Total Preparation Time"]+=job.metadata["total processing time"]
                        total_preparation_time+=job.metadata["total processing time"].total_seconds()
                        total_delivery_time+=(job.metadata["delivery_end_time"]-job.metadata["delivery_start_time"]).total_seconds()
                    else:
                        resturant_summary[job.restaurant_name]={"Orders Prepared":1,"Total Preparation Time":job.metadata["total processing time"]}
                        total_preparation_time+=job.metadata["total processing time"].total_seconds()
                        total_delivery_time+=(job.metadata["delivery_end_time"]-job.metadata["delivery_start_time"]).total_seconds()
                else:
                    not_prepared_orders+=1
        except Exception as e:
            print("Error in summary Calculations",str(e))
        try:
            with open(file_name,'w') as f:
                f.write("Summary Report\n")
                f.write("====================\n")
                for restaurant,summary in resturant_summary.items():
                    f.write(f"Restaurant: {restaurant}\n")
                    f.write(f"Orders Prepared: {summary['Orders Prepared']}\n")
                    f.write(f"Total Preparation Time: {summary['Total Preparation Time']}\n")
                    f.write("--------------------\n")
                f.write(f"Skipped Orders : {not_prepared_orders}\n")
                f.write(f"Number of total order in csv : {len(jobs)}\n")
                f.write(f"Number of Orders Prepared:{len(jobs)-not_prepared_orders}\n")
                f.write(f"Overall Total Preparation Time: {total_preparation_time}\n")
                f.write(f"Overall Total Delivery Time: {total_delivery_time}\n")
        except Exception as e:
            print(f"[Logger] ERROR writing summary report: {str(e)}")

    def _write_csv_report(self,filename,logs):
        try:
            if not logs:
                with open(filename, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['order_id', 'restaurant_name', 
                                                           'distance_km', 'prep_time_seconds', 
                                                           'prep_start_time', 'prep_end_time','delivery_start_time','delivery_end_time','delivery_by'])
                    writer.writeheader()
                return
            with open(filename, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=logs[0].keys())
                writer.writeheader()
                writer.writerows(logs)
        except IOError as e:
            print(f"[Logger] ERROR writing CSV file {filename}: {str(e)}")
        except Exception as e:
            print(f"[Logger] ERROR in _write_csv_report: {str(e)}")


class QueueManager:
    def __init__(self):
        self.queues={}
        self._buffer_bike=queue.Queue(maxsize=5)
        self._buffer_car=queue.Queue(maxsize=5)
        self._num_bike_delivery=0
        self._num_car_delivery=0
        self._lock=threading.Lock()
        self._distance_threshold=None
        self._num_bike_workers=None
        self._num_car_workers=None
        self._config={}
    def set_config(self,config):
        self._config=config
    def get_config(self):
        return self._config
    def add_bike_delivery(self,job):
        with self._lock:
            self._num_bike_delivery += 1
            self._buffer_bike.put(job)
    def add_car_delivery(self,job):
        with self._lock:
            self._num_car_delivery += 1
            self._buffer_car.put(job)
    def get_bike_order(self):
        return self._buffer_bike.get()
    def get_car_order(self):
        return self._buffer_car.get()
    def add_restaurant(self,restaurant):
        self.queues[restaurant]=queue.Queue(maxsize=7)
    def add_order(self,restaurant,order):
        self.queues[restaurant].put(order)
    def get_queue(self):
        return self.queues
    def get_order(self,restaurant):
        return self.queues[restaurant].get()

class Order_Producer(threading.Thread):
    def __init__(self,QueueManager,csv_file_name,available_restaurant,logger,jobs):
        try:
            super().__init__()
            self._QueueManager=QueueManager
            self.running=False
            self._csv_file_name=csv_file_name
            self._available_restaurant=available_restaurant
            self.logger=logger
            self._jobs=jobs
            self._lock=threading.Lock()
        except Exception as e:
            print(f"[Order_Producer] Error during intialization : {str(e)}")
    
    def run(self):
        self.start_processing()
    
    def start_processing(self):
        try:
            self.running=True    
            for restaurant in self._available_restaurant:
                self._QueueManager.add_restaurant(restaurant)
            with open(self._csv_file_name,'r') as file:
                orders=csv.DictReader(file)
                for order in orders:
                    job=OrderJob(
                        orderId=int(order['order_id']),
                        restaurant_name=order['restaurant_name'],
                        distance_km=float(order['distance_km']),
                        prep_time_seconds=int(order['prep_time_seconds']),
                        delivery_agent="",
                        metadata={}
                    )
                    if order['restaurant_name'] in self._QueueManager.get_queue().keys():
                        self._QueueManager.add_order(order['restaurant_name'],job)
                    else:
                        time.sleep(0.3)
                        job.metadata['prep_start_time']='None'
                        job.metadata['prep_end_time']='None'
                        job.metadata['delivery_start_time']='None'
                        job.metadata['delivery_end_time']='None'
                        job.metadata['order_delivered']=0
                        with self._lock:
                            self._jobs.append(job)

                for restaurant in self._available_restaurant:
                    self._QueueManager.add_order(restaurant,None)
        except Exception as e:
            print(f"[Order_Producer] Error in start method {e}")
    
    def wait_completion(self):
        try:
            queues=self._QueueManager.get_queue()
            for restaurant,queue in queues.items():
                queue.join()
        except Exception as e:
            print(f"[Manager] Error in wait_completion method {e}")

    def stop(self):
        self.running=False
        print("All reading stopped")

class RestaurantWorker(threading.Thread):

    def __init__(self,QueueManager,restaurant,threshold,logger):
        try:
            super().__init__()
            self._QueueManager=QueueManager
            self._restaurant=restaurant
            self._distance_threshold=threshold
            self._logger=logger
        except Exception as e:
            print(f"[Manager] Error during intialization : {str(e)}")

    def run(self):
        self.start_processing()

    def start_processing(self):
        try:
            while True:
                order=self._QueueManager.get_order(self._restaurant)
                if order is None:
                    print(f"{self._restaurant} completed it's all orders")
                    self._QueueManager.get_queue()[self._restaurant].task_done()
                    break
                processing_start_time=datetime.now()
                order.metadata['prep_start_time']=processing_start_time
                print(f"Order :{order.orderId} is being prepared by {order.restaurant_name}")
                time.sleep(order.prep_time_seconds)
                print(f"Order :{order.orderId} prepared by {order.restaurant_name}")
                processing_end_time=datetime.now()
                order.metadata['prep_end_time']=processing_end_time
                order.metadata['total processing time']=processing_end_time-processing_start_time
                if order.distance_km<=self._distance_threshold:
                    order.delivery_agent='Bike'
                    self._QueueManager.add_bike_delivery(order)
                else:
                    order.delivery_agent='Car'
                    self._QueueManager.add_car_delivery(order)
        except Exception as e:
            print(f"[RestaurantWorker] Error in start_processing method {e}")

    def stop(self):
        self.running=False
        print(f"{self._restaurant} stopped processing")

class Consumer(threading.Thread):
    def __init__(self,worker_id,thread_type,QueueManager,logger,jobs):
        super().__init__()
        self._lock=threading.Lock()
        self._logger=logger
        self._QueueManager=QueueManager
        self._worker_id=worker_id
        self._thread_type=thread_type
        self._jobs=jobs
        self.running=True

    def run(self):
        self.start_processing()

    def start_processing(self):
        try:
            while self.running:
                if self._thread_type=='Bike':
                    order=self._QueueManager.get_bike_order()
                    if order is None:
                        break
                    print(f"Delivery of {order.orderId} is started by Bike-{self._worker_id}")
                    start_time=datetime.now()
                    time.sleep(2*order.distance_km)
                    end_time=datetime.now()
                    order.metadata['delivery_start_time']=start_time
                    order.metadata['delivery_end_time']=end_time
                    order.metadata['order_delivered']=1
                    with self._lock:
                        self._jobs.append(order)
                elif self._thread_type=='Car':
                    order=self._QueueManager.get_car_order()
                    if order is None:
                        break
                    print(f"Delivery of {order.orderId} is started by Car-{self._worker_id}")
                    start_time=datetime.now()
                    time.sleep(2*order.distance_km)
                    end_time=datetime.now()
                    order.metadata['delivery_start_time']=start_time
                    order.metadata['delivery_end_time']=end_time 
                    order.metadata['order_delivered']=1
                    with self._lock:
                        self._jobs.append(order)
                else:
                    raise ValueError(f"Incorrect {self._thread.typr}")
        except ValueError as e:
            print("Error :",e)
    def stop(self):
        self.running=False

class JobManager:
    def __init__(self,configname):
        try:
            self._QueueManager=QueueManager()
            self._json_config=configname
            self.order_producer=[]
            self.restaurant_producer=[]
            self.consumer_agents=[]
            self._logger=Orderlogger()
            self._jobs=[]
        except Exception as e:
            print(f"[Manager] Error during initialization :{str(e)}")

    def read_config(self):
        try:
            with open(self._json_config,'r') as config_json:
                config_content=json.load(config_json)
                self._QueueManager.set_config(config_content)
        except FileNotFoundError:
            print("config json is not present")
            exit()
        except json.JSONDecodeError:
            print("failed to parse config file")
            exit()

    def start(self):
            try:
                self.read_config()
                requirement_fileds=["order_csv_file_name","available_restaurant_today","bike_agents","car_agents","distance_threshold"]
                config=self._QueueManager.get_config()
                for field in requirement_fileds:
                    if field not in config:
                        print(f"Missing required field {field} in config")
                        return
                distance_threshold=config['distance_threshold']
                csv_file_name=config['order_csv_file_name']
                num_bike_workers=config['bike_agents']
                num_car_workers=config['car_agents']
                available_restaurants=config['available_restaurant_today']
                if num_bike_workers==0 or num_car_workers==0:
                    raise ValueError("There is no one available for delivery")
                self.order_producer.append(Order_Producer(self._QueueManager,csv_file_name,available_restaurants,self._logger,self._jobs))
                for restaurant in available_restaurants:
                    producer=RestaurantWorker(self._QueueManager,restaurant,distance_threshold,self._logger)
                    self.restaurant_producer.append(producer)
                for i in range(1,num_bike_workers+1):
                    consumer=Consumer(i,'Bike',self._QueueManager,self._logger,self._jobs)
                    self.consumer_agents.append(consumer)
                for i in range(1,num_car_workers+1):
                    consumer=Consumer(i,'Car',self._QueueManager,self._logger,self._jobs)
                    self.consumer_agents.append(consumer)
                for producer in self.order_producer:
                    producer.start()
                for producer in self.restaurant_producer:
                    producer.start()
                for consumer in self.consumer_agents:
                    consumer.start()
            except ValueError as e:
                print(f"[JObmanager] Value Error in manager start : {str(e)}")
            except Exception as e:
                print(f"[JObmanager] Error in manager start : {str(e)}")

    def wait_completion(self):
        try:
            for producer in self.order_producer:
                producer.join()
            for producer in self.restaurant_producer:
                producer.join()
            for _ in range(self._QueueManager.get_config()['bike_agents']):
                self._QueueManager.add_bike_delivery(None)
            for _ in range(self._QueueManager.get_config()['car_agents']):
                self._QueueManager.add_car_delivery(None)
            for consumer in self.consumer_agents:
                consumer.join()
            self._logger.generate_reports(self._jobs)
            print("All orders are processed")

        except Exception as e:
            print(f"[Manager] Error in wait_completion method {e}")

    def stop(self):
        for producer in self.order_producer:
            producer.stop()
        for producer in self.restaurant_producer:
            producer.stop()
        for consumer in self.consumer_agents:
            consumer.stop()
        print("All works stopped")
        
def main():
    config_name=input("Enter config file name:")
    manager=JobManager(config_name)
    manager.start()
    manager.wait_completion()
if __name__=="__main__":
    main()



