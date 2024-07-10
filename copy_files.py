import os
import csv
import time
import shutil
import queue
import argparse
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait

def file_copy(src, dest):
    try:
        shutil.copy(src, dest)
        return None
    except Exception as e:
        return e

def copy_files(source_folder: str, destination_folder: str) -> None:
    # Define the names of your train, validation, and test CSV files
    csv_files = ["trn.csv", "val.csv", "tst.csv"]

    # keep track of copied file names for each subset (images and labels)
    tracker = defaultdict(lambda: {'images': set(), 'labels': set()})

    for csv_file in csv_files:
        csv_file_path = os.path.join(source_folder, csv_file)
        # print(csv_file_path)
        subset = f"{csv_file.split('.')[0]}"
        # Create subdirectories for "images" and "labels_burned"
        images_folder = os.path.join(destination_folder, f"{subset}/images")
        labels_burned_folder = os.path.join(destination_folder, f"{subset}/labels_burned")

        # Create the subdirectories if they don't exist
        os.makedirs(images_folder, exist_ok=True)
        os.makedirs(labels_burned_folder, exist_ok=True)
        
        if os.path.exists(csv_file_path):
            new_csv_file_path = os.path.join(destination_folder, f"new_{csv_file}")
            
            with open(csv_file_path, "r") as file, open(new_csv_file_path, "w", newline="") as new_file:
                csv_reader = csv.reader(file, delimiter=';')
                csv_writer = csv.writer(new_file, delimiter=';')
                
                with ThreadPoolExecutor() as executor:
                    # Use a thread pool for copying files
                    futures = []
                    for row in csv_reader:
                        # Assuming the file paths are in the "images" and "labels_burned" columns
                        if len(row) >= 2:  # Ensure there are at least three columns
                            # Assuming the file paths are in the first two columns
                            image_file_path = row[0]
                            labels_file_path = row[1]
                            annotation_percent = row[2]
                            # Extract the file names
                            image_file_name = os.path.basename(image_file_path)
                            labels_file_name = os.path.basename(labels_file_path)
                            
                            # New paths in the "images" and "labels_burned" subdirectories
                            new_image_path = os.path.join(f"{subset}/images", image_file_name)
                            new_labels_path = os.path.join(f"{subset}/labels_burned", labels_file_name)
                            
                            # Copy the image file to the "images" folder
                            if image_file_name in tracker[subset]['images']:
                                print(f"Duplicate exists for this image file: {image_file_name}")
                                print(f"------Duplicate file in subset: {subset}------")
                                print(f"------Duplicate soure: {image_file_path}------")
                                print(f"------Duplicate dest: {images_folder}------")
                                # shutil.copy(image_file_path, images_folder)
                            else:
                                # Submit the copy_file function as a thread for images
                                future_image = executor.submit(file_copy, image_file_path, images_folder)
                                futures.append(future_image)
                                tracker[subset]['images'].add(image_file_name)
                                
                                
                            # # Copy the labels file to the "labels_burned" folder
                            if labels_file_name in tracker[subset]['labels']:
                                print(f"Duplicate exists for this label file: {labels_file_name}")
                                # shutil.copy(labels_file_path, labels_burned_folder)
                                
                            else:
                                # Submit the copy_file function as a thread for labels
                                future_labels = executor.submit(file_copy, labels_file_path, labels_burned_folder)
                                futures.append(future_labels)
                                tracker[subset]['labels'].add(labels_file_name)

                            # Write the new paths to the new CSV file
                            new_row = [new_image_path, new_labels_path, annotation_percent]
                            csv_writer.writerow(new_row)
                    # Wait for all copy operations to complete
                    wait(futures)
        else:
            print(f"CSV file not found: {csv_file} make sure to save csv files as trn.csv, val.csv, tst.csv")
    
    print(f"Number of trn image/label: {len(tracker['trn']['labels'])}")
    print(f"Number of val image/label: {len(tracker['val']['labels'])}")
    print(f"Number of tst image/label: {len(tracker['tst']['labels'])}")

# def copy_file(src, dest, file_name, subset, tracker, lock):
#     try:
#         with lock:
#             if file_name not in tracker[subset]:
#                 shutil.copy(src, dest)
#                 tracker[subset].add(file_name)
#             else:
#                 print(f"------Duplicate exists for this file: {file_name}------")
#                 print(f"------Duplicate file in subset: {subset}------")
#                 print(f"------Duplicate soure: {src}------")
#                 print(f"------Duplicate dest: {dest}------")
                
#     except Exception as e:
#         print(f"Error copying file {file_name}: {e}")

# def copy_files(source_folder: str, destination_folder: str) -> None:
#     # Define the names of your train, validation, and test CSV files
#     csv_files = ["trn.csv", "val.csv", "tst.csv"]

#     # Initialize the tracker using defaultdict with sets
#     tracker = {}
#     lock = threading.Lock()

#     for csv_file in csv_files:
        
#         csv_file_path = os.path.join(source_folder, csv_file)
#         subset = f"{csv_file.split('.')[0]}"
#         tracker[subset] = set()
        
#         images_folder = os.path.join(destination_folder, f"{subset}/images")
#         labels_burned_folder = os.path.join(destination_folder, f"{subset}/labels_burned")
        
#         os.makedirs(images_folder, exist_ok=True)
#         os.makedirs(labels_burned_folder, exist_ok=True)
        
#         if os.path.exists(csv_file_path):
#             new_csv_file_path = os.path.join(destination_folder, f"new_{csv_file}")
            
#             with open(csv_file_path, "r") as file, open(new_csv_file_path, "w", newline="") as new_file:
#                 csv_reader = csv.reader(file, delimiter=';')
#                 csv_writer = csv.writer(new_file, delimiter=';')
                
#                 threads = []
#                 for row in csv_reader:
#                     if len(row) >= 2:
#                         image_file_path = row[0]
#                         labels_file_path = row[1]
#                         annotation_percent = row[2]
#                         image_file_name = os.path.basename(image_file_path)
#                         labels_file_name = os.path.basename(labels_file_path)
                        
#                         new_image_path = os.path.join(f"{subset}/images", image_file_name)
#                         new_labels_path = os.path.join(f"{subset}/labels_burned", labels_file_name)
                        
#                         # Create a thread for copying image file
#                         thread_image = threading.Thread(target=copy_file, args=(image_file_path, images_folder, image_file_name, subset, tracker, lock))
#                         thread_image.start()
#                         threads.append(thread_image)
                        
#                         # Create a thread for copying labels file
#                         thread_labels = threading.Thread(target=copy_file, args=(labels_file_path, labels_burned_folder, labels_file_name, subset, tracker, lock))
#                         thread_labels.start()
#                         threads.append(thread_labels)
                        
#                         new_row = [new_image_path, new_labels_path, annotation_percent]
#                         csv_writer.writerow(new_row)
                
#                 # Wait for all threads to complete
#                 for thread in threads:
#                     thread.join()
#         else:
#             print(f"CSV file not found: {csv_file} make sure to save csv files as trn.csv, val.csv, tst.csv")
    
#     print(f"Number of trn image/label: {len(tracker['trn'])}")
#     print(f"Number of val image/label: {len(tracker['val'])}")
#     print(f"Number of tst image/label: {len(tracker['tst'])}")

# Worker function for copying files
# def worker(q, tracker):
#     while True:
#         try:
#             src, dest, file_name, subset = q.get(timeout=1)  # Timeout for queue.get to prevent blocking indefinitely
#             copy_file(src, dest, file_name, subset, tracker)
#         except queue.Empty:
#             break
#         finally:
#             q.task_done()

# # Function to copy a file
# def copy_file(src, dest, file_name, subset, tracker):
#     try:
#         if file_name not in tracker[subset]:
#             shutil.copy(src, dest)
#             tracker[subset].add(file_name)
#         else:
#             print(f"Duplicate exists for this file: {file_name}")
#             print(f"------Duplicate exists for this file: {file_name}------")
#             print(f"------Duplicate file in subset: {subset}------")
#             print(f"------Duplicate soure: {src}------")
#             print(f"------Duplicate dest: {dest}------")
#     except Exception as e:
#         print(f"Error copying file {file_name}: {e}")

# # Main function for copying files
# def copy_files(source_folder: str, destination_folder: str, num_workers: int) -> None:
#     # Define the names of your train, validation, and test CSV files
#     csv_files = ["trn.csv", "val.csv", "tst.csv"]

#     # Initialize the tracker using a dictionary
#     tracker = {}

#     for csv_file in csv_files:
#         csv_file_path = os.path.join(source_folder, csv_file)
#         subset = f"{csv_file.split('.')[0]}"
#         tracker[subset] = set()
        
#         images_folder = os.path.join(destination_folder, f"{subset}/images")
#         labels_burned_folder = os.path.join(destination_folder, f"{subset}/labels_burned")
        
#         os.makedirs(images_folder, exist_ok=True)
#         os.makedirs(labels_burned_folder, exist_ok=True)
        
#         if os.path.exists(csv_file_path):
#             new_csv_file_path = os.path.join(destination_folder, f"new_{csv_file}")
            
#             with open(csv_file_path, "r") as file, open(new_csv_file_path, "w", newline="") as new_file:
#                 csv_reader = csv.reader(file, delimiter=';')
#                 csv_writer = csv.writer(new_file, delimiter=';')
                
#                 # Create a queue to hold tasks
#                 q = queue.Queue()
                
#                 # Create worker threads and start them
#                 threads = []
#                 for _ in range(num_workers):
#                     t = threading.Thread(target=worker, args=(q, tracker))
#                     t.start()
#                     threads.append(t)
                
#                 # Add tasks to the queue
#                 for row in csv_reader:
#                     if len(row) >= 2:
#                         image_file_path = row[0]
#                         labels_file_path = row[1]
#                         annotation_percent = row[2]
#                         image_file_name = os.path.basename(image_file_path)
#                         labels_file_name = os.path.basename(labels_file_path)
                        
#                         new_image_path = os.path.join(f"{subset}/images", image_file_name)
#                         new_labels_path = os.path.join(f"{subset}/labels_burned", labels_file_name)
                        
#                         q.put((image_file_path, images_folder, image_file_name, subset))
#                         q.put((labels_file_path, labels_burned_folder, labels_file_name, subset))
                        
#                         new_row = [new_image_path, new_labels_path, annotation_percent]
#                         csv_writer.writerow(new_row)
                
#                 # Wait for all tasks to be processed
#                 q.join()
                
#                 # Stop worker threads
#                 for _ in range(num_workers):
#                     q.put(None)  # Signal to stop worker thread
                
#                 for t in threads:
#                     t.join()
#         else:
#             print(f"CSV file not found: {csv_file} make sure to save csv files as trn.csv, val.csv, tst.csv")
    
#     print(f"Number of trn image/label: {len(tracker['trn'])}")
#     print(f"Number of val image/label: {len(tracker['val'])}")
#     print(f"Number of tst image/label: {len(tracker['tst'])}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy and create CSV files with new paths.")
    parser.add_argument("source_folder", help="Path to the source folder containing CSV files.")
    parser.add_argument("destination_folder", help="Path to the destination folder for copied files and new CSVs.")
    args = parser.parse_args()
    
    start_time = time.time()
    print(f'\n\nCopying files....\n\n')
    copy_files(args.source_folder, args.destination_folder)
    print("Elapsed time:{}".format(time.time() - start_time))