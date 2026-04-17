UPDATE document_block_assets
SET storage_path = 'uploads/' || storage_path
WHERE storage_path NOT LIKE 'uploads/%'
  AND asset_role = 'diagram_crop';
