from constants.colors import GREEN, RESET, YELLOW, DARK_GRAY
from constants.app_data import APP_NAME
import os
import logging
import subprocess

def print_outro(output_folder_path, file_sizes, file_word_counts, total_size, total_word_count, hours, minutes, seconds, download_bandwidth):

    """
    Print the outro message

    Args:
        output_folder_path (str): The path to the output folder
        file_sizes (dict): A dictionary of file sizes
        file_word_counts (dict): A dictionary of file word counts
        total_size (int): The total size of all files
        total_word_count (int): The total word count of all files
        hours (int): The number of hours taken to merge the files
        minutes (int): The number of minutes taken to merge the files
        seconds (int): The number of seconds taken to merge the files
        total_download_bandwidth (int): The total download bandwidth used
    """


    print()
    print("="*50)
    print(f"{GREEN}✅ Merge Completed!{RESET}")
    print(f"📁 Output Folder:{RESET} {output_folder_path}{RESET}")
    print(f"📋 Detailed File Report:{RESET}")
    for file_path, size in file_sizes.items():
        file_name = os.path.basename(file_path)
        size_mb = size / (1024 * 1024)
        word_count = file_word_counts[file_path]
        logging.info(f"File: {file_name}, Size: {size_mb:.2f}MB, Words: {word_count:,}")
        print(f"  • {file_name}{RESET} | {size_mb:.2f}MB | {word_count:,} words")
    
    # Log total size and word count of all files
    total_size_mb = total_size / (1024 * 1024)
    logging.info(f"Total size of all generated files: {total_size_mb:.2f}MB")
    logging.info(f"Total word count of all generated files: {total_word_count:,}")
    print(f"\n📊 Total size:{RESET} {total_size_mb:.2f}MB")
    print(f"📝 Total words:{RESET} {total_word_count:,}")
    total_seconds = hours * 3600 + minutes * 60 + seconds
    print(f"🕑 Total time taken: {hours:02d}:{minutes:02d}:{seconds:02d}")


    # Log download bandwidth
    download_bandwidth_mb = download_bandwidth / (1024 * 1024)
    logging.info(f"Total download bandwidth: {download_bandwidth_mb:.2f}MB")
    print(f"📶 Total download:{RESET} {download_bandwidth_mb:.2f}MB")
    print()

    # Calculate carbon footprint
    # Constants based on average estimates
    # Source: https://sustainablewebdesign.org/calculating-digital-emissions/
    # Average energy consumption for data transfer: ~0.06 kWh/GB
    # Average carbon intensity: ~442g CO2e/kWh (global average)
    # Cloud computing energy: ~0.01 kWh per minute of processing

    # Data transfer carbon footprint (includes both download and output)
    total_transfer_bytes = total_size + download_bandwidth
    total_transfer_gb = total_transfer_bytes / (1024 * 1024 * 1024)  # Convert bytes to GB
    data_transfer_energy_kwh = total_transfer_gb * 0.06  # kWh for data transfer
    data_transfer_carbon_g = data_transfer_energy_kwh * 442  # grams of CO2e

    # Processing carbon footprint
    processing_time_minutes = total_seconds / 60
    processing_energy_kwh = processing_time_minutes * 0.01  # kWh for processing
    processing_carbon_g = processing_energy_kwh * 442  # grams of CO2e

    # Total carbon footprint
    total_carbon_g = data_transfer_carbon_g + processing_carbon_g
    total_carbon_kg = total_carbon_g / 1000  # Convert to kg

    # Log carbon footprint
    logging.info(f"Estimated carbon footprint - Data transfer: {data_transfer_carbon_g:.2f}g CO2e")
    logging.info(f"Estimated carbon footprint - Processing: {processing_carbon_g:.2f}g CO2e")
    logging.info(f"Total estimated carbon footprint: {total_carbon_g:.2f}g CO2e ({total_carbon_kg:.6f}kg)")

    # Calculate individual components
    download_gb = download_bandwidth / (1024 * 1024 * 1024)
    download_carbon_g = (download_gb * 0.06) * 442
    
    output_gb = total_size / (1024 * 1024 * 1024)
    output_carbon_g = (output_gb * 0.06) * 442

    # Display carbon footprint information
    print(f"🌱 Carbon footprint estimation:")
    print(f"  • Download bandwidth: {download_carbon_g:.2f}g CO2e ({download_bandwidth_mb:.2f}MB)")
    print(f"  • Output generation: {output_carbon_g:.2f}g CO2e ({total_size_mb:.2f}MB)")
    print(f"  • Processing: {processing_carbon_g:.2f}g CO2e ({processing_time_minutes:.1f} minutes)")
    print(f"  • {GREEN}Total:{RESET} {total_carbon_g:.2f}g CO2e ({total_carbon_kg:.6f}kg)")

    # Add context to help understand the impact
    tree_absorption = 22000  # Average tree absorbs ~22kg CO2 per year
    equivalent_tree_minutes = (total_carbon_kg / tree_absorption) * 365 * 24 * 60  # Convert to minutes
    print(f"  • Equivalent to what an average tree absorbs in {equivalent_tree_minutes:.3f} minutes")
    print("\n")
    print(f"{YELLOW}Thank you for using the {APP_NAME}!{RESET}")
    print(f"\nDeveloped for the {DARK_GRAY}ArtIA experimental project{RESET} to help make sense")
    print("of the AI knowledge landscape by organizing and merging documents.")
    print()
    print("Made with ❤️ by @WilibertXXIV") #add collaborators here
    print("="*50 + "\n")

