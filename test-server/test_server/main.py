from dataclasses import dataclass
from pathlib import Path

from aiohttp import web


@dataclass
class ResourceHandler:
    dir: str

    async def serve(self, request):
        filepath = Path(self.dir) / request.match_info['filename']
        if not filepath.is_file():
            raise web.HTTPNotFound()

        headers = {}
        if '.protobuf' in filepath.suffixes:
            headers['X-Num-Entries'] = str(3)

        return web.FileResponse(filepath, headers=headers)


def main(args):
    handler = ResourceHandler(dir=args[0])

    app = web.Application()
    app.add_routes([web.get('/{filename}', handler.serve)])

    web.run_app(app)
