import matplotlib.pyplot as plt
import numpy as np
import os

def plot_grouped_bars(file_path):
    with open(file_path, 'r') as f:
        num_members = int(f.readline().strip())
        num_labels = int(f.readline().strip())
        x_labels = f.readline().strip().split()
        member_names = f.readline().strip().split()

        if len(x_labels) != num_labels:
            raise ValueError("Number of x labels does not match specified count.")
        if len(member_names) != num_members:
            raise ValueError("Number of member names does not match number of members.")

        # Read the next num_members lines as y-values
        data = []
        for _ in range(num_members):
            line = f.readline()
            if not line:
                raise ValueError("Not enough data lines for all members.")
            values = list(map(float, line.strip().split()))
            if len(values) != num_labels:
                raise ValueError("Number of y-values does not match number of x labels.")
            data.append(values)

    data = np.array(data)

    # Plot setup
    x = np.arange(num_labels)
    bar_width = 0.8 / num_members

    fig, ax = plt.subplots()
    for i in range(num_members):
        ax.bar(x + i * bar_width, data[i], width=bar_width, label=member_names[i])

    ax.set_xlabel('Benchmark', fontsize=18)
    ax.set_ylabel('Time Taken (s)', fontsize=18)
    ax.set_title('Comparing UDF Implementations', fontsize=20)
    ax.set_xticks(x + bar_width * (num_members - 1) / 2)
    ax.set_xticklabels(x_labels, fontsize=16)
    ax.legend()
    
    ax.grid(True, axis='y', linestyle='--', linewidth=0.5, alpha=0.7)

    plt.tight_layout()
    output_path = os.path.splitext(file_path)[0] + "_plot.png"
    plt.savefig(output_path, dpi=400, bbox_inches='tight')
    print(f"Plot saved as: {output_path}")
    plt.show()

if __name__ == "__main__":
    file_path = "./stats.txt" 
    plot_grouped_bars(file_path)
