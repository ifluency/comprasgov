-- Add created_at for compatibility with queries/clients that expect it.
-- Keep fetched_at as the canonical timestamp used by the ingestors.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'api_raw'
      AND column_name = 'created_at'
  ) THEN
    ALTER TABLE api_raw
      ADD COLUMN created_at timestamptz;
    UPDATE api_raw
      SET created_at = fetched_at
      WHERE created_at IS NULL;
    ALTER TABLE api_raw
      ALTER COLUMN created_at SET NOT NULL;
    ALTER TABLE api_raw
      ALTER COLUMN created_at SET DEFAULT now();
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_api_raw_created_at ON api_raw (created_at DESC);
