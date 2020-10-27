# Copyright 2018-2019 Geek Guild Co., Ltd.
# ==============================================================================

import glob
from PIL import Image
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.animation as animation
import os

"""Gif module.
"""

def generate_gif_animation(src_file_path_list, dst_file_path, interval=500, repeat_delay=1000):

    # prepare figure
    fig = plt.figure()

    # clearn the figure edge
    ax = plt.subplot(1, 1, 1)
    ax.spines['right'].set_color('None')
    ax.spines['top'].set_color('None')
    ax.spines['left'].set_color('None')
    ax.spines['bottom'].set_color('None')
    ax.tick_params(axis='x', which='both', top='off', bottom='off', labelbottom='off')
    ax.tick_params(axis='y', which='both', left='off', right='off', labelleft='off')

    image_list = []

    for src_file_path in src_file_path_list:
        image_file = Image.open(src_file_path)
        # spline36
        image_list.append([plt.imshow(image_file, interpolation="spline36")])

    # animation
    ani = animation.ArtistAnimation(fig, image_list, interval=interval, repeat_delay=repeat_delay)
    ani.save(dst_file_path)

if __name__ == '__main__':
    # sample
    src_dir_path = "/var/tensorflow/tsp/sample/animation/"
    src_file_path_list = glob.glob(src_dir_path + "*.png")
    generate_gif_animation(src_file_path_list, src_file_path_list)