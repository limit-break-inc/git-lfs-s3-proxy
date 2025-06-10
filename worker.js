import { AwsClient } from "aws4fetch";

const HOMEPAGE = "https://github.com/milkey-mouse/git-lfs-s3-proxy";
const EXPIRY = 3600;

const MIME = "application/vnd.git-lfs+json";

const METHOD_FOR = {
  "upload": "PUT",
  "download": "GET",
};

async function sign(s3, bucket, path, method) {
  const info = { method };
  const signed = await s3.sign(
    new Request(`https://${bucket}/${path}?X-Amz-Expires=${EXPIRY}`, info),
    { aws: { signQuery: true } }
  );
  return signed.url;
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname == "/") {
      if (request.method === "GET") {
        return Response.redirect(HOMEPAGE, 302);
      } else {
        return new Response(null, { status: 405, headers: { "Allow": "GET" } });
      }
    }

    if (!url.pathname.endsWith("/objects/batch")) {
      return new Response(null, { status: 404 });
    }

    if (request.method !== "POST") {
      return new Response(null, { status: 405, headers: { "Allow": "POST" } });
    }

    // Use secrets from environment variables
    let s3Options = {
      accessKeyId: env.ACCESS_KEY_ID,
      secretAccessKey: env.SECRET_ACCESS_KEY
    };

    const segments = url.pathname.split("/").slice(1, -2);
    let params = {};
    let bucketIdx = 0;
    for (const segment of segments) {
      const sliceIdx = segment.indexOf("=");
      if (sliceIdx === -1) {
        break;
      } else {
        const key = decodeURIComponent(segment.slice(0, sliceIdx));
        const val = decodeURIComponent(segment.slice(sliceIdx + 1));
        s3Options[key] = val;

        bucketIdx++;
      }
    }

    const s3 = new AwsClient(s3Options);
    const bucket = segments.slice(bucketIdx).join("/");
    const expires_in = params.expiry || env.EXPIRY || EXPIRY;

    const { objects, operation } = await request.json();
    const method = METHOD_FOR[operation];
    const response = JSON.stringify({
      transfer: "basic",
      objects: await Promise.all(objects.map(async ({ oid, size }) => ({
        oid, size,
        authenticated: true,
        actions: {
          [operation]: { href: await sign(s3, bucket, oid, method), expires_in },
        },
      }))),
    });

    return new Response(response, {
      status: 200,
      headers: {
        "Cache-Control": "no-store",
        "Content-Type": "application/vnd.git-lfs+json",
      },
    });
  }
};
