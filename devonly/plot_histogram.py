import matplotlib.pyplot as plt
import os
import numpy as np

def read_data(file_name):
    """
    Reads a file with numbers at the end of each line and returns a list of integers.
    """
    base_path = '/sphenix/u/sphnxpro/mainkolja/'
    file_path = os.path.join(base_path, file_name)
    
    data_list = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    parts = line.split()
                    num = int(parts[-1])
                    if num > 0:
                        data_list.append(num)
                except (ValueError, IndexError):
                    pass
    except FileNotFoundError:
        print(f"Warning: File not found at {file_path}")
        # Try relative path
        alt_path = f'../{file_name}'
        try:
            with open(alt_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        parts = line.split()
                        num = int(parts[-1])
                        if num > 0:
                            data_list.append(num)
                    except (ValueError, IndexError):
                        pass
        except FileNotFoundError:
             print(f"Error: File also not found at {alt_path}")
    return data_list


def plot_events_distribution():
    """
    Reads data from 'nums' and 'seednums' and plots their distributions,
    including mean and RMS in the legend. The y-axis is logarithmic.
    """
    tracks_list = read_data('nums')
    seeds_list = read_data('seednums')

    if not tracks_list and not seeds_list:
        print("No valid data found to plot.")
        return

    # Create histogram
    plt.figure(figsize=(10, 6))
    
    # Plot tracks
    if tracks_list:
        mean_tracks = np.mean(tracks_list)
        rms_tracks = np.sqrt(np.mean(np.square(tracks_list)))
        label_tracks = f'tracks (mean={mean_tracks:.2f}, rms={rms_tracks:.2f})'
        plt.hist(tracks_list, bins=100, range=(0, 1000), edgecolor='black', label=label_tracks)
        
    # Plot seeds on top
    if seeds_list:
        mean_seeds = np.mean(seeds_list)
        rms_seeds = np.sqrt(np.mean(np.square(seeds_list)))
        label_seeds = f'seeds (mean={mean_seeds:.2f}, rms={rms_seeds:.2f})'
        plt.hist(seeds_list, bins=100, range=(0, 1000), histtype='step', color='red', linewidth=2, label=label_seeds)

    plt.title('Distribution of Tracks and Seeds')
    plt.xlabel('Number of Events')
    plt.ylabel('Frequency (log scale)')
    plt.yscale('log')
    plt.grid(axis='y', alpha=0.75)
    plt.legend()

    # Save the plot
    output_filename = 'nevents_distribution.png'
    plt.savefig(output_filename)
    print(f"Histogram saved to {output_filename}")

if __name__ == '__main__':
    plot_events_distribution()
