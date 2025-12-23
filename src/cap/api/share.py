# cap/src/cap/api/share.py
"""
Share image endpoints.

Purpose:
- Accept an uploaded widget image (PNG/JPEG/WebP), store it on disk, keep metadata in DB.
- Return a public-but-unguessable URL (id + token query param) so social platforms can fetch it.
- Provide an OG/Twitter "share page" (HTML with meta tags) that references the image URL.
- Dedupe per user via sha256 (same bytes => same record).
"""

import hashlib, os, secrets, tempfile, json, re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import quote
from string import Template

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, Response
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import SharedImage, User
from cap.core.auth_dependencies import get_current_user_unconfirmed

router = APIRouter(prefix="/api/v1/share", tags=["share"])


_I18N_PATH = Path(__file__).resolve().parent / "share_i18n.json"

try:
    SHARE_I18N = json.loads(_I18N_PATH.read_text(encoding="utf-8"))
except Exception:
    SHARE_I18N = {}

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

DEFAULT_MAX_BYTES = 8 * 1024 * 1024  # 8 MB
DEFAULT_TTL_DAYS = 7
ALLOWED_MIMES_DEFAULT = "image/png,image/jpeg,image/webp"

MAX_BYTES = int(os.getenv("SHARE_IMAGE_MAX_BYTES", str(DEFAULT_MAX_BYTES)))
TTL_DAYS = int(os.getenv("SHARE_IMAGE_TTL_DAYS", str(DEFAULT_TTL_DAYS)))

ALLOWED_MIMES = set(
    m.strip().lower()
    for m in os.getenv("SHARE_IMAGE_ALLOWED_MIMES", ALLOWED_MIMES_DEFAULT).split(",")
    if m.strip()
)

# Where files are stored inside the container (bind-mounted on server)
SHARE_IMAGE_DIR = Path(os.getenv("SHARE_IMAGE_DIR", "/var/lib/cap/share-images")).resolve()

# Public base URL for absolute OG tags (your env already uses this)
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")


def _utcnow() -> datetime:
    # DB columns are TIMESTAMP WITHOUT TIME ZONE (naive). Keep everything naive UTC.
    return datetime.now()


def resolve_lang(request: Request) -> str:
    # 1) explicit query param
    qp = request.query_params.get("lang")
    if qp and qp.lower() in SHARE_I18N:
        return qp.lower()

    # 2) Accept-Language header
    al = request.headers.get("accept-language", "")
    for part in al.split(","):
        code = part.split(";")[0].strip().lower()
        if code in SHARE_I18N:
            return code
        # handle "pt-BR" -> "pt-br"
        if "-" in code:
            base = code.lower()
            if base in SHARE_I18N:
                return base

    # 3) fallback
    return "en"

def _ensure_storage_dir() -> None:
    try:
        print(f"[share] creating SHARE_IMAGE_DIR={SHARE_IMAGE_DIR}")
        SHARE_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Share image storage not available: {e}") from e


def _safe_mime(upload: UploadFile) -> str:
    mime = (upload.content_type or "").lower().strip()
    if mime not in ALLOWED_MIMES:
        raise HTTPException(status_code=415, detail=f"Unsupported image type: {mime or 'unknown'}")
    return mime


def _read_stream_to_tempfile_and_hash(
    upload: UploadFile, max_bytes: int, tmp_dir: Path
) -> Tuple[str, int, str]:
    """
    Stream the upload to a temp file (inside tmp_dir) while computing sha256.
    Returns: (tmp_path, total_bytes, sha256_hex)
    """
    h = hashlib.sha256()
    total = 0

    # IMPORTANT: Create temp file in the same filesystem as final destination
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="cap-share-", suffix=".bin", dir=str(tmp_dir))
    os.close(tmp_fd)

    try:
        with open(tmp_path, "wb") as f:
            while True:
                chunk = upload.file.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail=f"Image too large. Max {max_bytes} bytes.",
                    )
                h.update(chunk)
                f.write(chunk)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read upload: {e}") from e

    sha256_hex = h.hexdigest()
    return tmp_path, total, sha256_hex


def _build_image_path(image_id: str, token: str, etag: str) -> str:
    return f"/api/v1/share/image/{image_id}?t={token}&v={etag}"


def _build_page_path(image_id: str, token: str, etag: str) -> str:
    # v=etag is helpful so platforms refetch if the same id is ever reused (it shouldn't be, but safe)
    return f"/api/v1/share/page/{image_id}?t={token}&v={etag}"

def _abs_url_from_base(base: str, path: str) -> str:
    base = (base or "").rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}" if base else path

def _abs_url(path: str) -> str:
    """
    Convert a same-origin path into an absolute URL for OG tags.
    If PUBLIC_BASE_URL isn't set, fallback to the path (still works for some contexts).
    """
    if not path.startswith("/"):
        path = "/" + path
    if not PUBLIC_BASE_URL:
        return path
    return f"{PUBLIC_BASE_URL}{path}"


def _escape_attr(s: str) -> str:
    # minimal HTML attribute escaping
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


_PLACEHOLDER_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")
def render_shared_page(ctx: dict, template_html: str) -> str:
    def repl(m: re.Match) -> str:
        key = m.group(1)
        return str(ctx.get(key, m.group(0)))
    return _PLACEHOLDER_RE.sub(repl, template_html)


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------

@router.post("/image")
def upload_share_image(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_unconfirmed),
):
    """
    Upload a share image. Auth required.

    Returns:
    - url: image bytes URL
    - page_url: HTML page URL with OG/Twitter tags (best to share)
    """
    _ensure_storage_dir()
    mime = _safe_mime(file)

    tmp_path: Optional[str] = None
    try:
        tmp_path, total_bytes, sha256_hex = _read_stream_to_tempfile_and_hash(
            file, MAX_BYTES, SHARE_IMAGE_DIR
        )

        now = _utcnow()
        existing = (
            db.query(SharedImage)
            .filter(SharedImage.user_id == user.user_id)
            .filter(SharedImage.content_sha256 == sha256_hex)
            .filter(SharedImage.expires_at > now)
            .first()
        )
        if existing:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

            image_path = _build_image_path(existing.id, existing.access_token, existing.etag)
            page_path = _build_page_path(existing.id, existing.access_token, existing.etag)

            return {
                "url": image_path,
                "page_url": page_path,
                "absolute_url": _abs_url(image_path),
                "absolute_page_url": _abs_url(page_path),
                "expires_at": existing.expires_at.isoformat(),
                "etag": existing.etag,
                "bytes": existing.bytes,
                "mime": existing.mime,
                "deduped": True,
            }

        access_token = secrets.token_urlsafe(32)
        expires_at = now + timedelta(days=TTL_DAYS)
        etag = sha256_hex

        obj = SharedImage(
            user_id=user.user_id,
            access_token=access_token,
            content_sha256=sha256_hex,
            mime=mime,
            bytes=total_bytes,
            etag=etag,
            created_at=now,
            expires_at=expires_at,
            storage_path="__pending__",
        )
        db.add(obj)
        db.flush()

        ext = "png"
        if mime == "image/jpeg":
            ext = "jpg"
        elif mime == "image/webp":
            ext = "webp"

        final_name = f"{obj.id}.{ext}"
        final_path = SHARE_IMAGE_DIR / final_name

        try:
            os.replace(tmp_path, str(final_path))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to store image: {e}") from e

        obj.storage_path = final_name
        db.commit()

        image_path = _build_image_path(obj.id, obj.access_token, obj.etag)
        page_path = _build_page_path(obj.id, obj.access_token, obj.etag)

        return {
            "url": image_path,
            "page_url": page_path,
            "absolute_url": _abs_url(image_path),
            "absolute_page_url": _abs_url(page_path),
            "expires_at": obj.expires_at.isoformat(),
            "etag": obj.etag,
            "bytes": obj.bytes,
            "mime": obj.mime,
            "deduped": False,
        }

    except HTTPException:
        raise
    finally:
        if tmp_path:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass


@router.get("/image/{image_id}")
def get_share_image(
    image_id: str,
    request: Request,
    t: str,  # token query param (required)
    db: Session = Depends(get_db),
):
    """
    Public image fetch endpoint (no auth).
    """
    now = _utcnow()

    obj = db.query(SharedImage).filter(SharedImage.id == image_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")

    if not secrets.compare_digest(obj.access_token or "", t or ""):
        raise HTTPException(status_code=404, detail="Not found")

    if obj.expires_at <= now:
        raise HTTPException(status_code=404, detail="Expired")

    inm = request.headers.get("if-none-match")
    if inm and obj.etag and inm.strip('"') == obj.etag:
        return Response(status_code=304, headers={"ETag": obj.etag})

    _ensure_storage_dir()
    file_path = (SHARE_IMAGE_DIR / (obj.storage_path or "")).resolve()

    try:
        file_path.relative_to(SHARE_IMAGE_DIR)
    except Exception:
        raise HTTPException(status_code=500, detail="Invalid storage path")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Not found")

    max_age = int(max(0, (obj.expires_at - now).total_seconds()))
    max_age = min(max_age, 31536000)

    headers = {
        "ETag": obj.etag,
        "Cache-Control": f"public, max-age={max_age}",
    }

    return FileResponse(
        path=str(file_path),
        media_type=obj.mime,
        headers=headers,
        filename=file_path.name,
    )


@router.get("/page/{image_id}", response_class=HTMLResponse)
def get_shared_page(
    image_id: str,
    request: Request,
    t: str,
    db: Session = Depends(get_db),
    # Optional display params (frontend may pass; safe defaults otherwise)
    title: Optional[str] = Query(default=None),
    description: Optional[str] = Query(default=None),
    target_url: Optional[str] = Query(default=None),
    preview: bool = Query(default=False),

):
    """
    Public share page (HTML) that emits OG/Twitter meta tags.
    Social platforms fetch this HTML, read og:image, then fetch the image URL.

    - Uses the same token 't' and expiry checks as the image endpoint.
    - OG tags should use absolute URLs (PUBLIC_BASE_URL).
    """
    now = _utcnow()
    lang = resolve_lang(request)
    i18n = SHARE_I18N.get(lang, SHARE_I18N.get("en", {}))


    obj = db.query(SharedImage).filter(SharedImage.id == image_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")

    if not secrets.compare_digest(obj.access_token or "", t or ""):
        raise HTTPException(status_code=404, detail="Not found")

    if obj.expires_at <= now:
        raise HTTPException(status_code=404, detail="Expired")

    # Build URLs
    image_path = _build_image_path(obj.id, obj.access_token, obj.etag)
    page_path = _build_page_path(obj.id, obj.access_token, obj.etag)

    # For crawlers: PUBLIC_BASE_URL
    # For local manual testing: preview=1 uses the request host (localhost, etc.)
    base = PUBLIC_BASE_URL
    if preview or not PUBLIC_BASE_URL:
        base = str(request.base_url).rstrip("/")

    og_image = _abs_url_from_base(base, image_path)
    og_url = _abs_url_from_base(base, page_path)

    # If your SPA route (dashboard widget view) is known, you can pass it as target_url
    # and we’ll set og:site_name / og:url to the share page, while providing a normal link to the app.
    page_title = (title or i18n.get("default_title", "CAP")).strip()[:120]
    page_desc = (description or i18n.get(
        "default_description",
        "Explore data-driven insights on CAP",
    )).strip()[:300]

    # target = (target_url or PUBLIC_BASE_URL or "/").strip()
    target = PUBLIC_BASE_URL

    # Escape
    page_title_e = _escape_attr(page_title)
    page_desc_e = _escape_attr(page_desc)
    og_image_e = _escape_attr(og_image)
    og_url_e = _escape_attr(og_url)
    target_e = _escape_attr(target)


    tpl_path = Path(__file__).resolve().parent.parent / "shared_pages" / "shared_page.html"
    html = tpl_path.read_text(encoding="utf-8")

    ctx = {
        "title": page_title_e,
        "description": page_desc_e,
        "image_url": og_image_e,
        "target_url": target_e,
        "css_url": f"/share-static/shared_page.css",
        "t_shared_from_cap": i18n.get("shared_from_cap"),
        "t_chart_image_alt": i18n.get("chart_image_alt"),
        "t_open_in_cap": i18n.get("open_in_cap")
    }
    html = render_shared_page(ctx, html)
    
    # Cache HTML a bit, but not too long; crawlers will refetch anyway.
    headers = {
        "Cache-Control": "public, max-age=300",
        "X-Share-Lang": lang

    }
    return HTMLResponse(content=html, headers=headers)
