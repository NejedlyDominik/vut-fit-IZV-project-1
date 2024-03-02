import os
import errno
import argparse
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec
from download import DataDownloader


def plot_stat(data_source, fig_location=None, show_figure=False):
    """
    Load data and visualize them. Create a bar subgraph for every year, where on the x-axis are selected regions and on the y-axis numbers of accidents.
    Invividual bars in every subgraph are annotated with rank according to number of accidents in descending order, so the region with the most accidents 
    have the highest rank (in this case '1.', and so on). 

    Arguments:
        data_source     - already processed accidents data in specific format (tuple(list[str], list[np.ndarray]))
        fig_location    - address, where the figure is stored (default None - figure is not saved)
        show_figure     - if is set, the figure is shown in the window (default False)
    """
    if(fig_location == None and show_figure == False):
        # there is no need to do anything, if both of these arguments are not set
        return

    regions = data_source[1][0]
    years = data_source[1][6].astype('datetime64[Y]')

    uniq_regions = np.unique(regions)
    uniq_years = np.unique(years)
    
    # get numbers of accidents for each year in each region  
    statistics = [[np.count_nonzero((years == year) & (regions == region)) for region in uniq_regions] for year in uniq_years]

    rows, cols = get_row_col_num(len(uniq_years))

    # create the figure
    fig = plt.figure(constrained_layout=True, figsize=(11.75, 8.25))
    spec = gridspec.GridSpec(ncols=cols, nrows=rows, figure=fig)

    years_num = len(uniq_years)
    max_records = 0

    # get index of subplot with the mosts accidents in one region
    for i, row in enumerate(statistics):
        max_row = max(row)

        if(max_row > max_records):
            max_records = max_row
            max_index = i

    # create subplot with the mosts accidents in one region, because its y-axis has the largest range
    if(years_num != 0):
        ax_max = fig.add_subplot(spec[max_index])
    
    for i, year in enumerate(uniq_years):
        accidents = statistics[i]

        if(i != max_index):
            # every other subplot have the same range of y-axis as subplot with the mosts accidents in one region
            ax = fig.add_subplot(spec[i], sharey=ax_max)
        else:
            ax = ax_max

        ax.grid(axis='y', linewidth=0.5)
        ax.set_axisbelow(True)
        ax.tick_params(width=0)
        ax.set_title(year, fontsize=14)

        # add x label to the subplot, if its the last subplot in column
        if((i + cols) >= years_num):
            ax.set_xlabel('region shortcut', style='italic')

        # add y label to the subplot, if the subplot is in the first column
        if((i % cols) == 0):
            ax.set_ylabel('number of accidents', style='italic')
            
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        bars = ax.bar(uniq_regions, accidents, color='#E40000')

        ordered_accidents_nums = get_ordered_accidents(accidents)
        
        # rank individual bars in the subplot
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, height, ordered_accidents_nums[height], ha='center', va='bottom')

    fig.suptitle('Accidante rate in the Czech Republic', fontsize=16)

    if(fig_location != None):
        fig_location = os.path.normpath(fig_location)
        dir_name, fig_file = os.path.split(fig_location)

        # create directories
        if(dir_name != ''):
            try:
                os.makedirs(dir_name)
            except OSError as e:
                if(e.errno != errno.EEXIST):
                    raise
        
        fig.savefig(fig_location)

    if(show_figure):
        plt.show()


def get_row_col_num(graphs_num):
    """
    Count number of rows and cols for figure according to number of subplots.

    Arguments:
        graphs_num - number of subplots in figure

    Return value:
        int, int - number of rows and cols in this order
    """
    rows = 1
    cols = 1

    while((rows * cols) < graphs_num):
        if(cols < rows):
            cols += 1
        else:
            rows += 1

    return rows, cols


def get_ordered_accidents(accidents):
    """
    Rank numbers of accidents in descending order.

    Arguments:
        accidents - list of numbers of accidents in regions

    Return value:
        dict() - where key is number of accidents (int) and value is rank (str)
    """
    accidents.sort(reverse=True)
    ordered_nums = {}

    for order, acc_num in enumerate(accidents, start=1):
        ordered_nums[acc_num] = f'{order}.'

    return ordered_nums


if(__name__ == "__main__"):
    # if the script is run as main, using script arguments it is possible to set fig_location and show_figure parameters from command line
    parser = argparse.ArgumentParser()
    parser.add_argument('--fig_location', type=str, default=None)
    parser.add_argument('--show_figure', action='store_true')

    args = parser.parse_args()
   
    data_source = DataDownloader().get_list(['PHA', 'STC', 'JHC', 'PLK', 'ULK', 'HKK', 'JHM', 'MSK', 'OLK', 'ZLK', 'VYS', 'PAK', 'LBK', 'KVK'])
    plot_stat(data_source, args.fig_location, args.show_figure)