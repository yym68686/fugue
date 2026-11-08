FROM node:24-bookworm-slim@sha256:01d566e8d94d8c619591d78652babfccd3c6992a3dffa9ed5ab39e11cf6c70e9 AS build
WORKDIR /opt/fugue-workers
RUN npm install --no-audit --no-fund --save-exact workerd@__WORKERD_VERSION__ esbuild@__ESBUILD_VERSION__
WORKDIR /src
COPY . .
COPY .fugue-workers/build.mjs /opt/fugue-workers/build.mjs
RUN node /opt/fugue-workers/build.mjs /src /out

FROM node:24-bookworm-slim@sha256:01d566e8d94d8c619591d78652babfccd3c6992a3dffa9ed5ab39e11cf6c70e9
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/fugue-workers
COPY --from=build /opt/fugue-workers/node_modules ./node_modules
COPY --from=build /out ./app
COPY .fugue-workers/launch.mjs ./launch.mjs
USER node
EXPOSE 8080
CMD ["node", "launch.mjs"]
