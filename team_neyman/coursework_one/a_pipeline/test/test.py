import sys
from pathlib import Path

root_path = Path(__file__).resolve().parent.parent
sys.path.append(str(root_path))

from modules.url_parser import dolthub_pipeline

if __name__ == '__main__':
    dolthub_pipeline.update_earnings_data(r"C:/Ryan/UCL BDF/Big Data in Quantitative Finance/ift_coursework_2025")