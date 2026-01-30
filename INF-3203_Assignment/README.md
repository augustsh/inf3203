# INF-3203 Assignment

This repository contains the code required for the INF-3203 assignment.

## File Structure

```text
INF-3203_Assignment/
├── classify.py          # Main image classification script (GoogLeNet)
├── requirements.txt     # Python dependencies
├── README.md            # Project documentation
└── .gitignore           # Git ignore file
```

## Script files

The script you will be using for the assignment is `classify.py`. This script uses a pre-trained GoogLeNet model to classify images. Feel free to modify the output format so it fits your implementation. The core functionality should remain the same.

## Image files

The images used in this assignment can be found in the `/share/inf3203/unlabeled_images` directory on the IFI Cluster.
Please **DO NOT** copy these images into you own directory, as there are 1.281.167 JPEG files in the directory.

## Usage

1. Set up a virtual environment and install the required dependencies:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   pip install -r requirements.txt
   ```
   i.e.
   ```bash
   python classify.py /share/inf3203/unlabeled_images/119.JPEG
   ```

2. Run the classification script with an input image:

   ```bash
   python classify.py <input_image>
   ```

This will print the top predicted label for the input image to the console.
