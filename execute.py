import argparse
from pathlib import Path

from ASFINT.Pipeline.workflow import run
from ASFINT.Utility.Utils import ensure_folder

def main(manual=None, args=None):
    if manual is not None:
        # Programmatic call, no CLI parsing
        run(
            pull_path=manual.get('pullPath'), 
            push_path=manual.get('pushPath'), 
            process_type=manual.get('processType'),
        )
        return
    
    parser = argparse.ArgumentParser(description="Run ASUCProcessor pipeline locally.")
    parser.add_argument(
        "--input", "-i", type=str, default="files/input",
        help="Path to input folder or file"
    )
    parser.add_argument(
        "--output", "-o", type=str, default="files/output",
        help="Path to output folder"
    )
    parser.add_argument(
        "--process", "-p", type=str, default="fr",
        help="Type of processing (e.g., fr, absa, agenda)"
    )
    
    args = parser.parse_args()

    # Resolve paths
    pull_path = Path(args.input).resolve()
    push_path = Path(args.output).resolve()

    # Create folders if missing
    if pull_path.is_dir():
        ensure_folder(pull_path)
    ensure_folder(push_path)

    # Run pipeline
    run(pull_path=str(pull_path), push_path=str(push_path), process_type=args.process)

if __name__ == "__main__":
    MANUAL = False
    if MANUAL:
        settings = {
            'pullPath': '/Users/jonathanngai/Desktop/ASUC Research/ASFIN/files/input', 
            'pushPath': '/Users/jonathanngai/Desktop/ASUC Research/ASFIN/files/output', 
            'processType': 'FR'
        }
        main(manual=settings)
    main()
