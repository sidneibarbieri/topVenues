# Immutable profile

This repository preserves the declarations for four immutable profiles. The
current profile is bundled; historical binaries remain in their original tags
and are fetched only on request so a reviewer does not download redundant data.

| Profile | Records | Venues | Role |
| --- | ---: | ---: | --- |
| `security-20` | 20,305 | 20 | Frozen SBSeg-SF artifact profile; archived at `v1.0.1` |
| `security-20-v2` | 14,863 | 20 | Deduplicated successor; archived at `v1.1.0` |
| `security-20-v3` | 14,859 | 20 | Strict-window, identity-adjudicated predecessor |
| `security-20-v4` | 14,859 | 20 | Bundled title-repaired current release |

The profile configuration declares the venue keys and target years. Its manifest declares the compressed snapshot SHA-256 and observed data counts. Validate it with:

```bash
python scripts/verify_profile_snapshot.py --profile security-20-v4
```

Fetch and verify a historical binary before reproducing it:

```bash
python scripts/fetch_archived_profile.py --profile security-20
python scripts/verify_profile_snapshot.py --profile security-20
```

A refreshed scope is a new profile and release, not an in-place change to this snapshot.
