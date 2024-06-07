## Copyright (c) 2022, Lancaster University
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions
## are met:
##
## 1. Redistributions of source code must retain the above copyright
##    notice, this list of conditions and the following disclaimer.
##
## 2. Redistributions in binary form must reproduce the above
##    copyright notice, this list of conditions and the following
##    disclaimer in the documentation and/or other materials provided
##    with the distribution.
##
## 3. Neither the name of the copyright holder nor the names of its
##    contributors may be used to endorse or promote products derived
##    from this software without specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
## FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
## COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
## INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
## (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
## SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
## HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
## STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
## OF THE POSSIBILITY OF SUCH DAMAGE.

import yaml

_ALLOWED_NAMES = [ ]

def get_field(fld_spec):
    fld = { 'name': fld_spec['name'] }

    svals = fld_spec.get('values')
    if svals is not None:
        if len(svals) < 1:
            return None
        fld['vals'] = svals
        return fld

    maxv = fld_spec.get('max')
    if maxv is not None:
        minv = fld_spec.get('min', 0)
        stepv = fld_spec.get('step', 1)
        fld['vals'] = list(range(minv, maxv + stepv, stepv))
        if len(fld['vals']) < 1:
            return None
        return fld

    return None

def get_array(spec):
    mapping = { }
    path_fmt = spec.get('path')
    if path_fmt is None:
        return mapping

    ## Get field specifications.  Each field has a name, and either
    ## integer min, max and optional step, or array of string values.
    flds = []
    for fld_spec in spec.get('fields', []):
        fld = get_field(fld_spec)
        if fld is not None:
            flds.append(fld)
        continue

    ## Load labels and computed labels.
    lbls = spec.get('labels', { })
    global _ALLOWED_NAMES
    allowed_names = set(_ALLOWED_NAMES)
    allowed_names.update(set(fld['name'] for fld in flds))
    clbls = { }
    for lnam, ldef in spec.get('computed_labels', { }).items():
        code = compile(ldef, '<string>', 'eval')
        for con in code.co_names:
            if con not in allowed_names:
                raise NameError(f'bad name in expr: {con}')
            continue
        else:
            clbls[lnam] = code
            pass
        continue

    ## Load label formats.
    fmt_spec = spec.get('formats', { })
    fmts = { }
    for k in set(lbls.keys()).union(set(clbls.keys())):
        fmts[k] = fmt_spec.get(k, '%s')
        continue

    ## Iterate over all fields.  Set all digits to 0, and all fields
    ## to their first values.  Also include the allowed names.
    digs = [ 0 ] * len(flds)
    vals = { fld['name']: fld['vals'][0] for fld in flds }
    vals.update(_ALLOWED_NAMES)
    while True:
        ## Apply the current values for form the path.
        path = path_fmt.format(**vals)

        ## Build the label set for this path.
        lblset = { k: { 'value': v, 'fmt': fmts[k] } for k, v in lbls.items() }
        for lnam, code in clbls.items():
            lval = eval(code, { "__builtins__": {} }, vals)
            lblset[lnam] = { 'value': lval, 'fmt': fmts[lnam] }
            continue
        mapping[path] = lblset

        ## Increment.
        for i in range(len(digs)):
            digs[i] += 1
            if digs[i] < len(flds[i]['vals']):
                ## This digit value is acceptable, so update the
                ## corresponding field, and break.
                vals[flds[i]['name']] = flds[i]['vals'][digs[i]]
                break
            ## This digit has reached its radix.  It needs to be
            ## reset, and the corresponding field updated, before we
            ## move on to the next digit.
            digs[i] = 0
            vals[flds[i]['name']] = flds[i]['vals'][digs[i]]
            continue
        else:
            ## The most significant digit reached its radix.
            break
        continue

    return mapping

def get_layout_patterns(doc):
    res = { }
    for lyt, spec in doc.get('patterns', { }).items():
        res[lyt] = get_array(spec)
        continue
    return res

def get_layouts(doc, pats):
    res = { }
    for lytn, spec in doc.get('layouts', { }).items():
        lyt = { }
        for i in spec:
            lyt.update(pats.get(i, { }))
            continue
        res[lytn] = lyt
        continue
    return res
    

if __name__ == '__main__':
    import sys
    from pprint import pprint, pformat
    doc = yaml.load(sys.stdin, Loader=yaml.SafeLoader)
    drive_spec = doc.get('drive_paths', { })
    pats = get_layout_patterns(drive_spec)
    pprint(pats)
    lyts = get_layouts(drive_spec, pats)
    pprint(lyts)
    pass

