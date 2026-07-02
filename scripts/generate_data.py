#!/usr/bin/env python3
"""Wrapper script to execute mock data generation."""
import argparse
from pathlib import Path
from src.extract.data_generator import EcommerceDataGenerator

def main():
    parser = argparse.ArgumentParser(description="Generate mock data for e-commerce pipeline")
    parser.add_argument("--volume", type=int, default=1000, help="Number of orders to generate")
    parser.add_argument("--output", type=str, default="data/raw", help="Output directory")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Generating data with scale volume={args.volume}...")
    generator = EcommerceDataGenerator(volume=args.volume)
    generator.save_to_csv(output_dir)
    print(f"Mock data successfully written to {output_dir}")

if __name__ == "__main__":
    main()
