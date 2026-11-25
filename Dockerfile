ARG NODE_VERSION=24.22.0
FROM node:${NODE_VERSION}-slim

LABEL fly_launch_runtime="Node.js"

WORKDIR /
ENV NODE_ENV="production"

COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY server.js server-cloudflare.js ./

EXPOSE 8080
CMD ["npm", "run", "start"]
