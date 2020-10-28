# Copyright 2018-2019 Geek Guild Co., Ltd.
# ==============================================================================

import sys
import matplotlib.pyplot as plt

sys.path.append("python/")

def merge_png(png_path_1, png_path_2, merged_path):
    # load image to np.array
    img_1 = plt.imread(png_path_1)

    img_2 = plt.imread(png_path_2)

    img_merged = (img_1 + img_2) * 0.5

    # save image
    plt.imsave(merged_path, img_merged)


if __name__ == '__main__':
    dir_path = '/var/tensorflow/lpf/report/'
    png_path_1 = dir_path + 'test_plot_1.png'
    png_path_2 = dir_path + 'test_plot_2.png'
    merged_path = dir_path + 'test_plot_1-2.png'
    merge_png(png_path_1, png_path_2, merged_path)
