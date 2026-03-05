import csv
import threading
import time
import queue
from dataclasses import dataclass
import os


@dataclass
class PrintJob():
    customer_name:str
    file_name:str
    print_duration:int
class Logger:
    def __init__(self):
        self._normal_task_count=0
        self._heavy_task_count=0
        self._lock=threading.Lock()
        self._lock2=threading.Lock()
        self.normal_printer_logs=[['User name','File name','Duration','Start timestamp','End timestamp','total_time']]
        self.heavy_printer_logs=[['User name','File name','Duration','Start timestamp','End timestamp','total_time']]
        self.normal_summary=[]
        self.heavy_summary=[]
    def normal_summary_append(self,log):
        with self._lock:
            self.normal_summary.append(log)
    def heavy_summary_append(self,log):
        with self._lock2:
            self.heavy_summary.append(log)
    def increment_normal(self):
        with self._lock:
            self._normal_task_count+=1
            return self._normal_task_count
    def increment_heavy(self):
        with self._lock2:
            self._heavy_task_count+=1
            return self._heavy_task_count
    def get_normal_count(self):
        with self._lock:
            return self._normal_task_count
    def get_heavy_count(self):
        with self._lock2:
            return self._heavy_task_count
    def append_normal_log(self,log):
        with self._lock:
            self.normal_printer_logs.append(log)
    def append_heavy_log(self,log):
        with self._lock2:
            self.heavy_printer_logs.append(log)

    
        
class CSVJobProducer(threading.Thread):
    def __init__(self,name,threshold,buffer_heavy,buffer_normal,logger_instance):
        super().__init__()
        self.file_name=name
        self.print_tasks=[]
        self.threshold=threshold
        self.buffer_heavy = buffer_heavy
        self.buffer_normal = buffer_normal
        self.logger_instance = logger_instance
    def read_csv(self):
        try:
            with open(f'{self.file_name}','r') as file:
                reader=csv.reader(file)
                next(reader)
                for x in reader:
                    if len(x)!=3:
                        raise ValueError("Data must have customer_name,file_name,print_duration_seconds")
                    job:PrintJob=PrintJob(x[0],x[1],int(x[2]))
                    self.print_tasks.append(job)
        except FileNotFoundError:
            print("There is no file with name",self.file_name)
        except ValueError as e:
            print("CSV formate error",e)

    def run(self):
        self.read_csv()
        for task in self.print_tasks:
            try:
                duration=int(task.print_duration)
            except (ValueError,TypeError):
                print("Invalid print_duration")
                return
            try:
                if duration > self.threshold:
                    print(f"[Producer] {task.customer_name} classified as Heavy")
                    self.buffer_heavy.put(task)
                else:
                    print(f"[Producer] {task.customer_name} classified as Normal")
                    self.buffer_normal.put(task)
            except queue.Full:
                print(f"[Producer] {task.customer_name} waiting (Queue Full)")
        self.buffer_normal.put(None)
        self.buffer_heavy.put(None)

class PrinterWorker(threading.Thread):
    def __init__(self,name,printer_type,buffer_normal,buffer_heavy,logger_instance):
        super().__init__()
        self.name=name
        self.printer_type=printer_type
        self._buffer_normal=buffer_normal
        self._buffer_heavy=buffer_heavy
        self.logger_instance = logger_instance
    def run(self):
        while True:
            if self.printer_type=='Heavy':
                work=self._buffer_heavy.get()
                if work is None:
                    self._buffer_heavy.put(work)
                    break
                start_time_heavy=time.time()
                print(f'[Heavy-{self.logger_instance.get_heavy_count()+1}] Printing {work.customer_name} ({work.print_duration})............')
                time.sleep(int(work.print_duration))
                print(f'Completed {work.customer_name} ({work.print_duration})')
                end_time_heavy=time.time()
                self.logger_instance.append_heavy_log([work.customer_name,work.file_name,work.print_duration,start_time_heavy,end_time_heavy,end_time_heavy-start_time_heavy,self.name])
                self.logger_instance.increment_heavy()
            else:
                work=self._buffer_normal.get()
                if work is None:
                    self._buffer_normal.put(work)
                    break
                start_time_normal=time.time()
                print(f'[Normal-{self.logger_instance.get_normal_count()+1}] Printing {work.customer_name} ({work.print_duration})...........')
                time.sleep(int(work.print_duration))
                print(f'Completed {work.customer_name} ({work.print_duration})')
                end_time_normal=time.time()
                self.logger_instance.append_normal_log([work.customer_name,work.file_name,work.print_duration,start_time_normal,end_time_normal,end_time_normal-start_time_normal,self.name])
                self.logger_instance.increment_normal()

class PrintManager():
    def __init__(self,file_name,normal_printer,heavy_printer,threshold,logger_instance):
        self.file_name=file_name
        self.normal_printer=normal_printer
        self.heavy_printer=heavy_printer
        self._buffer_normal=queue.Queue(maxsize=7)
        self._buffer_heavy=queue.Queue(maxsize=7)
        self.threshold=threshold
        self.logger_instance=logger_instance
        self.jobProducer=CSVJobProducer(self.file_name,self.threshold,self._buffer_heavy,self._buffer_normal,logger_instance)
        self.normal_printer_start=0
        self.normal_printer_end=0
        self.heavy_printer_start=0
        self.heavy_printer_end=0

    def create_threads(self):
        self.normal_printers=[PrinterWorker(f'normal_printer{i}','Normal',self._buffer_normal,self._buffer_heavy,self.logger_instance) for i in range(self.normal_printer)]
        self.heavy_printers=[PrinterWorker(f'heavy_printer{i}','Heavy',self._buffer_normal,self._buffer_heavy,self.logger_instance) for i in range(self.heavy_printer)]

    def start_threads(self):
        self.jobProducer.start()
        self.normal_printer_start=time.time()
        for thread in self.normal_printers:
            thread.start()
        self.heavy_printer_start=time.time()
        for thread in self.heavy_printers:
            thread.start()


    def join_threads(self):
        for thread in self.normal_printers:
            thread.join()
        self.normal_printer_end=time.time()
        for x in [['Total number of tasks',self.logger_instance.get_normal_count()],['Starting of normal printer',self.normal_printer_start],['Ending of normal printer',self.normal_printer_end],[ 'Total time taken by normal printer',self.normal_printer_end - self.normal_printer_start]]:
            self.logger_instance.normal_summary_append(x)
        for thread in self.heavy_printers:
            thread.join()
        self.heavy_printer_end=time.time()
        for x in [['Total number of tasks',self.logger_instance.get_heavy_count()],['Starting of heavy printer',self.heavy_printer_start],['Ending of heavy printer',self.heavy_printer_end],['Total time taken by heavy printer',self.heavy_printer_end - self.heavy_printer_start]]:
            self.logger_instance.heavy_summary_append(x)

    def create_csv(self,logs,name):
        try:
            with open(name,mode='w',newline='') as file:
                writer=csv.writer(file)
                writer.writerows(logs)
        except Exception as e:
            print(f"Not able to generate csv: {e}")
    def create_txt(self,summary,name):
        try:
            with open(name,mode='w') as file:
                for line in summary:
                    file.write(f"{line[0]}: {line[1]}\n")
        except Exception as e:
            print(f"Not able to generate txt: {e}")

def read_input(prompt):
    while True:
        try:
            value = int(input(prompt))
            return value
        except ValueError:
            print("Invalid input. Please enter an integer.")

if __name__=='__main__':
    print_tasks=[]
    print("----WELCOME------")
    file_name=input("Enter CSV File Name :")
    if not os.path.isfile(file_name):
        print("given file name not exist")
        exit()
    threshold=read_input("Enter value of threshold:")
    normal_printers=read_input("Enter number of normal printers :")
    heavy_printers=read_input("Enter Number of heavy printers:")
    logger_instance=Logger()
    print_manager1=PrintManager(file_name,normal_printers,heavy_printers,threshold,logger_instance)
    print_manager1.create_threads()
    print_manager1.start_threads()
    print_manager1.join_threads()
    print_manager1.create_csv(logger_instance.normal_printer_logs,"Normal.csv")
    print_manager1.create_csv(logger_instance.heavy_printer_logs,"Heavy.csv")
    print_manager1.create_txt(logger_instance.normal_summary,"Normal_summary.txt")
    print_manager1.create_txt(logger_instance.heavy_summary,"Heavy_summary.txt")
    print("------------all completed---------------")