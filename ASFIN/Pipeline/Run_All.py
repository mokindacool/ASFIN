from ASFIN.Pipeline.Execute import execute

import argparse

def run_all():
    parser = argparse.ArgumentParser()
    parser.add_argument("--testing", action="store_true", help=f"Run in testing mode.")
    parser.add_argument("--no-verbose", dest="verbose", action="store_false", help="Disable verbose logging")
    parser.add_argument("--no-drive", dest="drive", action="store_false", help="Disable Google Drive processing")
    parser.add_argument("--no-bigquery", dest="bigquery", action="store_false", help="Disable BigQuery push")
    parser.add_argument("--halt-push", dest="haltpush", action="store_true", help="Disables pushing cleaned files to Google Drive")

    parser.set_defaults(verbose=True, drive=True, bigquery=True, testing=False, haltpush=False)
    args = parser.parse_args()

    for dataset in ["ABSA", "OASIS", "FR", "CONTINGENCY"]:
        execute(
            t=dataset, 
            verbose=args.verbose, 
            drive=args.drive, 
            bigquery=args.bigquery, 
            testing=args.testing, 
            haltpush=args.haltpush
        )

if __name__ == "__main__":
    run_all()