/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of CMU, TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#pragma once

#include "pdlfs-common/port.h"
#include "pdlfs-common/slice.h"

namespace pdlfs {
namespace {
// Transform a 64-bit (8-byte) integer into a 12-byte filename.
const char base64_table[] =
    "+-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
inline Slice Base64Enc(char* const dst, uint64_t input) {
  input = htobe64(input);
  const unsigned char* in = reinterpret_cast<unsigned char*>(&input);
  char* p = dst;
  *p++ = base64_table[in[0] >> 2];
  *p++ = base64_table[((in[0] & 0x03) << 4) | (in[1] >> 4)];
  *p++ = base64_table[((in[1] & 0x0f) << 2) | (in[2] >> 6)];
  *p++ = base64_table[in[2] & 0x3f];

  *p++ = base64_table[in[3] >> 2];
  *p++ = base64_table[((in[3] & 0x03) << 4) | (in[4] >> 4)];
  *p++ = base64_table[((in[4] & 0x0f) << 2) | (in[5] >> 6)];
  *p++ = base64_table[in[5] & 0x3f];

  *p++ = base64_table[in[6] >> 2];
  *p++ = base64_table[((in[6] & 0x03) << 4) | (in[7] >> 4)];
  *p++ = base64_table[(in[7] & 0x0f) << 2];
  *p++ = '+';
  assert(p - dst == 12);
  return Slice(dst, p - dst);
}
}  // namespace
}  // namespace pdlfs
