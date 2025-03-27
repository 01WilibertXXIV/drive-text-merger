import time
import sys

class ProgressBar:
    def __init__(self, total_subfolders):
        self.total_subfolders = total_subfolders
        self.start_time = time.time()

    def update(self, current_subfolder, current_index, detailed_output=None):
        """
        Update the progress bar and optionally display detailed output
        
        :param current_subfolder: Name of the current subfolder being processed
        :param current_index: Current index of subfolder being processed
        :param detailed_output: Optional detailed output to display before progress bar
        """
        # Calculate progress
        elapsed_time = time.time() - self.start_time
        progress_percentage = (current_index / self.total_subfolders) * 100

        # Clear entire line
        sys.stdout.write('\r' + ' ' * 100)
        sys.stdout.flush()

        # Print detailed output if provided
        if detailed_output:
            print(f"({current_index}/{self.total_subfolders}) - Searching in {current_subfolder}")
            print(detailed_output)

        # Progress bar
        progress_bar_width = 50
        filled_width = int(progress_percentage / 100 * progress_bar_width)
        bar = '=' * filled_width + '-' * (progress_bar_width - filled_width)
        
        # Write progress bar at the bottom
        sys.stdout.write(f'\r[{bar}] {progress_percentage:.1f}% | Elapsed: {elapsed_time:.2f}s')
        sys.stdout.flush()