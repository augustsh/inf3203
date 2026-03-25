import sys
import cv2
import urllib
from typing import List
import numpy as np
from PIL import Image
import torch
from torchvision import transforms
from torchvision.models import GoogLeNet_Weights


class ImageClassificationPipeline:
    """Image classification pipeline using a pre-trained GoogLeNet model."""

    def __init__(self) -> None:
        """Initialize the image classification pipeline."""
        self.device = "cpu"
        self.tfms = transforms.Compose(
            [
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )

        self.model = (
            torch.hub.load(
                "pytorch/vision:v0.10.0", "googlenet", weights=GoogLeNet_Weights.DEFAULT
            )
            .eval()
            .to(self.device)
        )

        for param in self.model.parameters():
            param.requires_grad = False

        self.categories = self._load_imagenet_labels()

    def _load_imagenet_labels(self) -> List[str]:
        """Load ImageNet labels from a remote file.

        Returns:
            List[str]: List of ImageNet class labels.
        """
        url = (
            "https://raw.githubusercontent.com/pytorch/hub/master/imagenet_classes.txt"
        )
        try:
            with urllib.request.urlopen(url) as f:
                return [s.decode("utf-8").strip() for s in f.readlines()]

        except Exception as e:
            print(f"Could not download labels file: {e}")
            return [f"class_{i}" for i in range(1000)]

    def __call__(self, img: np.ndarray) -> str:
        """Classify an image and return the predicted label.

        Args:
            img (np.ndarray): Input image in BGR format.

        Returns:
            str: Predicted label.
        """

        # Convert BGR to RGB and process
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        img_pil = Image.fromarray(img_rgb)
        input_tensor = self.tfms(img_pil)
        input_batch = input_tensor.unsqueeze(0).to(self.device)

        # Predict
        with torch.no_grad():
            output = self.model(input_batch)

        probabilities = torch.nn.functional.softmax(output[0], dim=0)
        _, top_catid = torch.topk(probabilities, 1)

        top_label = self.categories[top_catid[0]]

        return top_label



def main():
    classifier = ImageClassificationPipeline()

    for line in sys.stdin:
        path = line.strip()
        if not path:
            continue
        try:
            frame = cv2.imread(path)
            if frame is None:
                print(f"WARNING: could not read {path}", file=sys.stderr)
                continue
            label = classifier(frame)
            print(f"{path}\t{label}", flush=True)
        except Exception as e:
            print(f"WARNING: failed to classify {path}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()