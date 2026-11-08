import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {decryptToken} from '../src/crypto.ts';
import {decryptFernet} from '../src/fernet.ts';
const vector=JSON.parse(readFileSync(new URL('../tests_py/token-vector.json',import.meta.url),'utf8'));
test('existing TypeScript collector can read credentials encrypted by Python',async()=>{
  assert.equal(await decryptToken(vector.python_v2,vector.key),vector.plaintext);
  assert.equal(await decryptFernet(vector.python_fernet,vector.key+'='),vector.plaintext);
});
