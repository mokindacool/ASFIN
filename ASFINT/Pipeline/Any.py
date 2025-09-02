# In AEOCFO/Pipeline/Any.py

from ASFIN.Pipeline.Execute import execute
import argparse

def run(process=None, args=None):
    """
    If `process` is given, run directly with that process,
    else parse CLI args and run accordingly.
    
    `args` can be list of args for testing or internal use.
    """

    if process is not None:
        # Programmatic call, no CLI parsing
        execute(
            t=process,
            verbose=True,
            drive=True,
            bigquery=True,
            testing=False,
            haltpush=False
        )
        return

    # CLI mode - parse args
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--testing", action="store_true")
    parser.add_argument("--no-verbose", dest="verbose", action="store_false")
    parser.add_argument("--no-drive", dest="drive", action="store_false")
    parser.add_argument("--no-bigquery", dest="bigquery", action="store_false")
    parser.add_argument("--halt-push", dest="haltpush", action="store_true")
    parser.set_defaults(verbose=True, drive=True, bigquery=True, testing=False, haltpush=False)

    parsed_args = parser.parse_args(args)

    execute(
        t=parsed_args.dataset.upper(),
        verbose=parsed_args.verbose,
        drive=parsed_args.drive,
        bigquery=parsed_args.bigquery,
        testing=parsed_args.testing,
        haltpush=parsed_args.haltpush
    )

if __name__ == "__main__":
    run()
