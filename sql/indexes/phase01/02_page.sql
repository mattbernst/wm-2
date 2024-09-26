CREATE UNIQUE INDEX IF NOT EXISTS `page_id_unique` ON `page` (`id` ASC);
CREATE INDEX IF NOT EXISTS `page_type_idx` ON `page` (`page_type` ASC);

