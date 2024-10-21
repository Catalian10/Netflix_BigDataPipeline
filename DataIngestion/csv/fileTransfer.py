import subprocess
import os

def transfer_to_hdfs(local_paths, hdfs_path):
    """
    Transfer multiple files from local to HDFS, overwriting if they exist.

    :param local_paths: list - A list of paths to files on the local file system
    :param hdfs_path: str - The destination path on HDFS
    """
    for local_path in local_paths:
        # Check if the local path exists
        if not os.path.exists(local_path):
            print(f"Error: The local path '{local_path}' does not exist.")
            continue  # Skip this file and move to the next

        # Check if the HDFS path already exists and delete if necessary
        check_command = f"hadoop fs -test -e {hdfs_path}/{os.path.basename(local_path)}"
        try:
            subprocess.run(check_command, shell=True, check=True)
            # If the command succeeds, the path exists, so we remove it
            delete_command = f"hadoop fs -rm {hdfs_path}/{os.path.basename(local_path)}"
            subprocess.run(delete_command, shell=True, check=True)
            print(f"Existing HDFS file '{hdfs_path}/{os.path.basename(local_path)}' deleted.")
        except subprocess.CalledProcessError:
            # If the command fails, it means the path does not exist, and we can proceed
            print(f"No existing HDFS file '{hdfs_path}/{os.path.basename(local_path)}' found. Proceeding with upload.")

        # Construct the hadoop put command
        put_command = f"hadoop fs -put {local_path} {hdfs_path}/"

        try:
            # Execute the command
            subprocess.run(put_command, shell=True, check=True)
            print(f"Successfully transferred '{local_path}' to '{hdfs_path}/'.")
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while transferring '{local_path}': {e}")

# Example usage
if __name__ == "__main__":
    # Specify your local file paths and HDFS path
    local_file_paths = [
        "/home/ec2-user/UKUSSeptBatch/Siddhesh/Project/DataIngestion/Movielens/movies.csv",  # Change these
        "/home/ec2-user/UKUSSeptBatch/Siddhesh/Project/DataIngestion/Movielens/links.csv"   # Change these
    ]
    hdfs_destination_path = "ukussept2024/Sid/Project/NewApproach1710/DataIngestion/csvFiles"  # Change this

    transfer_to_hdfs(local_file_paths, hdfs_destination_path)

