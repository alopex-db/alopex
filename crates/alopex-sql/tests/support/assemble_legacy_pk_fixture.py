"""Use the existing byte-preserving assembler for the six separate PK cases."""
import assemble_legacy_unique_fixture as assembler

def main():
    assembler.CASES = (
        "pk_first_valid", "pk_later_valid",
        "pk_first_null_attempt", "pk_later_null_attempt",
        "pk_first_duplicate_attempt", "pk_later_duplicate_attempt",
    )
    assembler.main()

if __name__ == "__main__":
    main()
