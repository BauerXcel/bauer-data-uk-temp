
import subprocess
import time
import datetime
def run_reporter_jar(vendor, year, month):
    """
    Runs the Java Reporter.jar command for a given vendor, year, and month.
    """
    print(f"Running Report: Vendor {vendor} - Year {year} - Month {month}")
    
    command = [
        "java", "-jar", "Reporter.jar", "p=Reporter.properties",
        "Finance.getReport", f"{vendor},", "ZZ,", "AppleNews+,", f"{year},", f"{month}"
    ]
     
    print("Executing Command:", " ".join(command))
    # try:
    #     result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
    # except subprocess.CalledProcessError as e:
    #     print(f"Error running the command: {' '.join(command)}")
    #     print(f"Return Code: {e.returncode}")
    time.sleep(5)   
def main():
    """
    Main function to loop through vendors and generate reports.
    """
    vendors = ["91956116", "92889957","85029379"]
    years = [str(datetime.datetime.now().year),
            str(datetime.datetime.now().year - 1)]
    
    # today = datetime.datetime.now()
     
    # if today.month == 1:  
    #     previous_year = str(today.year - 1)
    #     previous_month = "12"
    # else:
    #     previous_year = str(today.year)
    #     previous_month = str(today.month - 1).zfill(2)  
    # for vendor in vendors:
    #     run_reporter_jar(vendor, previous_year, previous_month)
    
    # for vendor in vendors:
    #     run_reporter_jar(vendor, current_year, current_month)  
    for vendor in vendors:
        for year in years:
            for month in range(1, 13):   
                run_reporter_jar(vendor, year, month)
if __name__ == "__main__":
    main()
