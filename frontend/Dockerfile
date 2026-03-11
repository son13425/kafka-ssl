FROM node:22.11.0
WORKDIR /app
COPY package*.json ./
RUN npm install -g npm@11.6.0
RUN npm install
COPY . ./
RUN cat .env.frontend > .env
RUN npm run build
CMD cp -r dist result_dist