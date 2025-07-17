import { Metadata } from '../generated/index.js';

export const binaryTransformer = (
  metadata: Metadata,
  row: Buffer | null,
): Buffer | null => {
  const binary_buff = row;
  return binary_buff == null ? null : binary_buff;
};
