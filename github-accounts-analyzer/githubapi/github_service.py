from datetime import datetime
from github import Github
from loguru import logger
from githubapi.commit_model import CommitModel


class GitHubService():

    def __init__(self, github_api, commit_model: CommitModel):
        self.github_api = github_api
        self.commit_model = commit_model

    def pollCommits(self, login: str, starting_datetime: str) -> list:
        commits = self.search_commits(login, starting_datetime)
        logger.debug(commits.totalCount)
        return [self.get_commit_dict(c) for c in commits]

    def get_commit_dict(self, commit) -> dict:
        full_name = self.get_repo_full_name_from_comments_url(commit.comments_url)
        repo = self.get_repo(full_name)
        return self.commit_model.to_dict(commit=commit, repository=repo)

    def search_commits(self, login: str, starting_datetime: str) -> list:
        return self.github_api.search_commits(
            f'author:{login} author-date:>{starting_datetime}')

    def get_repo(self, full_name: str):
        return self.github_api.get_repo(full_name)

    def get_repo_full_name_from_comments_url(self, url: str) -> str:
        # get full_name from string 'https://api.github.com/repos/user/repo/commits/sha'
        repo_full_name = '/'.join(url.split('/')[4:6])
        return repo_full_name


# if __name__ == '__main__':
#     github_token = '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'
#     github_api = Github(github_token)
#     github_service = GitHubService(github_api, CommitModel)
#     result = github_service.pollCommits('kopagm', datetime(2021, 11, 20).isoformat())
#     logger.info(result)