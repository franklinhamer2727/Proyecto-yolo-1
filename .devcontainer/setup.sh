#!/bin/bash

echo "Installing system dependencies..."

sudo apt update

sudo apt install -y \
    ffmpeg \
    libsm6 \
    libxext6 \
    libgl1 \
    build-essential \
    wget \
    curl \
    git

echo "Creating virtual environment..."

python -m venv venv
source venv/bin/activate

echo "Installing Python dependencies..."

pip install --upgrade pip

pip install \
    numpy \
    pandas \
    opencv-python-headless==4.10.0.84 \
    ultralytics \
    pytest \
    pylint

echo "Setup complete"
