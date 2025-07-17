// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import { Metadata } from '../generated/index.js';
import { codepageByLanguageId, codepageBySortId } from '../codepages.js';
import * as iconv from 'iconv-lite';

export const nCharNVarCharTransformer = (
  metadata: Metadata,
  row: Buffer | null,
): string | null => {
  const nvarchar_buff = row;
  return nvarchar_buff == null ? null : nvarchar_buff.toString('ucs2');
};

export const varCharTransformer = (
  metadata: Metadata,
  row: Buffer | null,
): string | null => {
  const varchar_buff = row;
  let encoding = undefined;
  if (metadata.encoding != null) {
    if (metadata.encoding.isUtf8) {
      encoding = 'utf8';
    } else if (metadata.encoding.sortId !== 0) {
      encoding = codepageBySortId[metadata.encoding.sortId];
    } else {
      encoding = codepageByLanguageId[metadata.encoding.lcidLanguageId];
    }
  } else {
    encoding = 'utf8';
  }
  return varchar_buff == null ? null : iconv.decode(varchar_buff, encoding);
};
